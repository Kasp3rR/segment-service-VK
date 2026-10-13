package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/distribution"
	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type Store interface {
	DispatchOne(context.Context, func(context.Context, domain.Batch) error) (bool, error)
	ProcessBatch(context.Context, domain.Batch, domain.Delivery) (bool, error)
	RecordRetry(context.Context, domain.Batch, error) error
	FailJob(context.Context, domain.Batch, error) error
	Quarantine(context.Context, domain.Delivery, []byte, error) error
}
type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}
type Worker struct {
	Store                          Store
	Brokers                        []string
	Topic, Group, Instance         string
	Logger                         *zap.Logger
	PollInterval, OperationTimeout time.Duration
}
type BatchBalancer struct{}

func (BatchBalancer) Balance(m kafka.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}
	index := m.Partition % len(partitions)
	if index < 0 {
		index = 0
	}
	return partitions[index]
}
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: BatchBalancer{}, RequiredAcks: kafka.RequireAll, BatchSize: 1, MaxAttempts: 3, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second}
}
func (w Worker) NewReader() *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{Brokers: w.Brokers, Topic: w.Topic, GroupID: w.Group, MinBytes: 1, MaxBytes: 10 << 20, CommitInterval: 0, StartOffset: kafka.FirstOffset, GroupBalancers: []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}}, SessionTimeout: 10 * time.Second, HeartbeatInterval: 2 * time.Second, MaxWait: 500 * time.Millisecond})
}
func Send(ctx context.Context, writer *kafka.Writer, b domain.Batch) error {
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	return writer.WriteMessages(ctx, kafka.Message{Key: []byte(b.JobID), Partition: b.Number, Value: raw})
}
func Decode(raw []byte) (domain.Batch, error) {
	var b domain.Batch
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&b); err != nil {
		return b, domain.ErrInvalidMessage
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return b, domain.ErrInvalidMessage
	}
	return b, distribution.ValidateBatch(b)
}
func (w Worker) Handle(ctx context.Context, m kafka.Message, consumerID string) error {
	d := domain.Delivery{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, ConsumerID: consumerID}
	b, err := Decode(m.Value)
	if err == nil {
		_, err = w.Store.ProcessBatch(ctx, b, d)
	}
	if errors.Is(err, domain.ErrInvalidMessage) {
		return w.Store.Quarantine(ctx, d, m.Value, err)
	}
	if errors.Is(err, domain.ErrBatchFailed) {
		return w.Store.FailJob(ctx, b, err)
	}
	if err != nil && distribution.ValidJobID(b.JobID) {
		_ = w.Store.RecordRetry(ctx, b, err)
	}
	return err
}
func Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	return min(time.Duration(1<<min(attempt, 6))*100*time.Millisecond, 5*time.Second)
}
func Pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (w Worker) Consume(ctx context.Context, r Reader, consumerID string) {
	defer r.Close()
	for ctx.Err() == nil {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.Logger.Warn("kafka fetch failed", zap.String("consumer", consumerID), zap.Error(err))
			if !Pause(ctx, time.Second) {
				return
			}
			continue
		}
		for attempt := 0; ctx.Err() == nil; attempt++ {
			operation, cancel := context.WithTimeout(ctx, w.OperationTimeout)
			err = w.Handle(operation, m, consumerID)
			if err == nil {
				err = r.CommitMessages(operation, m)
			}
			cancel()
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return
			}
			w.Logger.Warn("kafka message retry", zap.String("consumer", consumerID), zap.Int("partition", m.Partition), zap.Int64("offset", m.Offset), zap.Error(err))
			if !Pause(ctx, Backoff(attempt)) {
				return
			}
		}
	}
}
func (w Worker) Publish(ctx context.Context, writer *kafka.Writer) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, w.OperationTimeout)
		found, err := w.Store.DispatchOne(operation, func(ctx context.Context, b domain.Batch) error {
			publishing, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			return Send(publishing, writer, b)
		})
		cancel()
		if err != nil && ctx.Err() == nil {
			w.Logger.Warn("outbox retry", zap.Error(err))
		}
		if !found || err != nil {
			if !Pause(ctx, w.PollInterval) {
				return
			}
		}
	}
}
func (w Worker) Run(ctx context.Context) {
	writer := NewWriter(w.Brokers, w.Topic)
	defer writer.Close()
	var group sync.WaitGroup
	group.Add(1)
	go func() { defer group.Done(); w.Publish(ctx, writer) }()
	for i := 0; i < domain.Consumers; i++ {
		id := fmt.Sprintf("%s/%d", w.Instance, i)
		group.Add(1)
		go func() { defer group.Done(); w.Consume(ctx, w.NewReader(), id) }()
	}
	group.Wait()
}
func CheckTopic(ctx context.Context, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return errors.New("no Kafka brokers configured")
	}
	dialer := kafka.Dialer{Timeout: 5 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err = conn.SetDeadline(deadline); err != nil {
		return err
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return err
	}
	if len(partitions) < domain.Consumers {
		return fmt.Errorf("topic %s needs at least %d partitions; got %d", topic, domain.Consumers, len(partitions))
	}
	return nil
}
