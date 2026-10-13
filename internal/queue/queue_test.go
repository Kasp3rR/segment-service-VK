package queue

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type fakeStore struct {
	errors                                []error
	events                                *[]string
	handled, quarantined, failed, retried int
	persistError                          error
}

func (f *fakeStore) DispatchOne(context.Context, func(context.Context, domain.Batch) error) (bool, error) {
	return false, nil
}
func (f *fakeStore) ProcessBatch(context.Context, domain.Batch, domain.Delivery) (bool, error) {
	f.handled++
	if f.events != nil {
		*f.events = append(*f.events, "process")
	}
	if len(f.errors) > 0 {
		err := f.errors[0]
		f.errors = f.errors[1:]
		return false, err
	}
	return true, nil
}
func (f *fakeStore) RecordRetry(context.Context, domain.Batch, error) error { f.retried++; return nil }
func (f *fakeStore) FailJob(context.Context, domain.Batch, error) error {
	f.failed++
	return f.persistError
}
func (f *fakeStore) Quarantine(context.Context, domain.Delivery, []byte, error) error {
	f.quarantined++
	return f.persistError
}
func validMessage() kafka.Message {
	raw, _ := json.Marshal(domain.Batch{Version: 1, JobID: "11111111-2222-4333-8444-555555555555", SegmentID: 1, Number: 0, UserIDs: []int64{1}})
	return kafka.Message{Topic: "test", Partition: 0, Offset: 7, Value: raw}
}

type fakeReader struct {
	events           *[]string
	cancel           context.CancelFunc
	fetches, commits int
	commitFailures   int
	closed           bool
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	r.fetches++
	*r.events = append(*r.events, "fetch")
	if r.fetches > 1 {
		return kafka.Message{}, errors.New("unexpected next fetch")
	}
	return validMessage(), nil
}
func (r *fakeReader) CommitMessages(context.Context, ...kafka.Message) error {
	r.commits++
	*r.events = append(*r.events, "commit")
	if r.commits <= r.commitFailures {
		return errors.New("commit unavailable")
	}
	r.cancel()
	return nil
}
func (r *fakeReader) Close() error { r.closed = true; return nil }
func TestConsumerRetriesBeforeAdvancingOffset(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	events := []string{}
	store := &fakeStore{events: &events, errors: []error{errors.New("database unavailable")}}
	reader := &fakeReader{events: &events, cancel: cancel, commitFailures: 1}
	w := Worker{Store: store, Logger: zap.NewNop(), OperationTimeout: time.Second}
	w.Consume(ctx, reader, "consumer-0")
	want := []string{"fetch", "process", "process", "commit", "process", "commit"}
	if !reflect.DeepEqual(events, want) || reader.fetches != 1 || !reader.closed || store.retried != 1 {
		t.Fatalf("events=%v reader=%+v retries=%d", events, reader, store.retried)
	}
}
func TestHandleMessageFailures(t *testing.T) {
	for _, tc := range []struct {
		name                        string
		err                         error
		quarantine, failed, retried int
	}{{"success", nil, 0, 0, 0}, {"mismatched_payload", domain.ErrInvalidMessage, 1, 0, 0}, {"permanent", domain.ErrBatchFailed, 0, 1, 0}, {"transient", errors.New("database unavailable"), 0, 0, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{errors: []error{tc.err}}
			w := Worker{Store: store}
			err := w.Handle(context.Background(), validMessage(), "0")
			if (err != nil) != (tc.retried > 0) || store.quarantined != tc.quarantine || store.failed != tc.failed || store.retried != tc.retried {
				t.Fatalf("%+v %v", store, err)
			}
		})
	}
}
func TestQuarantineMustCommit(t *testing.T) {
	store := &fakeStore{persistError: errors.New("database unavailable")}
	w := Worker{Store: store}
	m := validMessage()
	m.Value = []byte("bad json")
	if err := w.Handle(context.Background(), m, "0"); err == nil || store.quarantined != 1 || store.handled != 0 {
		t.Fatal("quarantine error ignored")
	}
}
func TestFailureStateMustCommit(t *testing.T) {
	store := &fakeStore{errors: []error{domain.ErrBatchFailed}, persistError: errors.New("database unavailable")}
	if err := (Worker{Store: store}).Handle(context.Background(), validMessage(), "0"); err == nil {
		t.Fatal("failure state persistence ignored")
	}
}
func TestDecodeStrictMessage(t *testing.T) {
	valid := validMessage().Value
	for _, raw := range [][]byte{[]byte("null"), []byte("{"), append(append([]byte{}, valid...), []byte(" {}")...), []byte(`{"version":1,"unexpected":true}`)} {
		if _, err := Decode(raw); !errors.Is(err, domain.ErrInvalidMessage) {
			t.Fatal(err)
		}
	}
	if _, err := Decode(valid); err != nil {
		t.Fatal(err)
	}
}
func TestDeterministicPartitionRouting(t *testing.T) {
	b := BatchBalancer{}
	for i := 0; i < 12; i++ {
		if got := b.Balance(kafka.Message{Partition: i}, 0, 1, 2, 3); got != i%4 {
			t.Fatal(got)
		}
	}
	if b.Balance(kafka.Message{}, 7) != 7 || b.Balance(kafka.Message{}) != 0 || b.Balance(kafka.Message{Partition: -1}, 2, 3) != 2 {
		t.Fatal("invalid partition mapping")
	}
}
func TestBackoffBounded(t *testing.T) {
	if Backoff(-1) != 100*time.Millisecond || Backoff(1) != 200*time.Millisecond || Backoff(100) != 5*time.Second {
		t.Fatal("bad backoff")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if Pause(ctx, time.Hour) {
		t.Fatal("cancellation ignored")
	}
	if !Pause(context.Background(), time.Nanosecond) {
		t.Fatal("timer not fired")
	}
}
func TestTopicRequiresBrokers(t *testing.T) {
	if err := CheckTopic(context.Background(), nil, "x"); err == nil {
		t.Fatal("empty brokers accepted")
	}
}
