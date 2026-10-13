//go:build integration && kafka

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
	"github.com/Kasp3rR/segment-service-VK/internal/queue"
)

type kafkaFixture struct {
	s      *postgres.Store
	w      queue.Worker
	client *kafka.Client
	writer *kafka.Writer
}

func kafkaSetup(t *testing.T, partitions int) *kafkaFixture {
	t.Helper()
	raw := os.Getenv("TEST_KAFKA_BROKERS")
	if raw == "" {
		t.Fatal("TEST_KAFKA_BROKERS is required")
	}
	brokers := strings.Split(raw, ",")
	topic := fmt.Sprintf("stage34-%d", time.Now().UnixNano())
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 5 * time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = resp.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
	})
	s := fixture(t, true)
	w := queue.Worker{Store: s, Brokers: brokers, Topic: topic, Group: topic + "-group", Instance: "integration", Logger: zap.NewNop(), PollInterval: 20 * time.Millisecond, OperationTimeout: 15 * time.Second}
	writer := queue.NewWriter(brokers, topic)
	t.Cleanup(func() { _ = writer.Close() })
	return &kafkaFixture{s: s, w: w, client: client, writer: writer}
}
func eventually(t *testing.T, why string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(35 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("timed out: " + why)
}
func (f *kafkaFixture) consumers(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); f.w.Consume(ctx, f.w.NewReader(), fmt.Sprintf("integration/%d", i)) }(i)
	}
	t.Cleanup(func() {
		cancel()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Error("consumers did not stop")
		}
	})
	eventually(t, "four stable Kafka group members", func() bool {
		resp, err := f.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: []string{f.w.Group}})
		if err != nil || len(resp.Groups) != 1 {
			return false
		}
		g := resp.Groups[0]
		if g.Error != nil || g.GroupState != "Stable" || len(g.Members) != 4 {
			return false
		}
		seen := map[int]bool{}
		for _, m := range g.Members {
			n := 0
			for _, topic := range m.MemberAssignments.Topics {
				if topic.Topic == f.w.Topic {
					for _, p := range topic.Partitions {
						seen[p] = true
						n++
					}
				}
			}
			if n != 1 {
				return false
			}
		}
		return len(seen) == 4
	})
	return ctx
}
func (f *kafkaFixture) publish(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		found, err := f.s.DispatchOne(ctx, func(ctx context.Context, b domain.Batch) error { return queue.Send(ctx, f.writer, b) })
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			return
		}
	}
}
func (f *kafkaFixture) completed(t *testing.T, j domain.Job) {
	t.Helper()
	eventually(t, "completed job", func() bool {
		got, err := f.s.GetDistribution(context.Background(), j.ID)
		return err == nil && got.Status == "completed" && got.AssignedUsers == j.PlannedUsers
	})
}
func (f *kafkaFixture) offset(partition int) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := f.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: f.w.Group, Topics: map[string][]int{f.w.Topic: {partition}}})
	if err != nil || resp.Error != nil {
		return -1
	}
	for _, p := range resp.Topics[f.w.Topic] {
		if p.Partition == partition && p.Error == nil {
			return p.CommittedOffset
		}
	}
	return -1
}
func TestKafkaFourConsumersParallelBatches(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 20000)
	seg := createSegment(t, f.s, "FOUR")
	j := startJob(t, f.s, seg.ID, "100")
	if _, err := f.s.Pool.Exec(context.Background(), `CREATE FUNCTION slow_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN NEW; END $$; CREATE TRIGGER slow_batch BEFORE INSERT ON processed_batches FOR EACH ROW EXECUTE FUNCTION slow_batch()`); err != nil {
		t.Fatal(err)
	}
	f.consumers(t)
	f.publish(t)
	f.completed(t, j)
	var consumers, partitions, n int
	var spread float64
	if err := f.s.Pool.QueryRow(context.Background(), `SELECT count(*),count(DISTINCT consumer_id),count(DISTINCT kafka_partition),extract(epoch FROM max(processed_at)-min(processed_at))::float8 FROM processed_batches`).Scan(&n, &consumers, &partitions, &spread); err != nil {
		t.Fatal(err)
	}
	if n != 4 || consumers != 4 || partitions != 4 || spread >= 2 {
		t.Fatalf("batches=%d consumers=%d partitions=%d start_spread=%fs", n, consumers, partitions, spread)
	}
	if countRows(t, f.s, "user_segments") != 20000 {
		t.Fatal("missing assignments")
	}
	t.Logf("4 consumers, 4 partitions, 4 x 5000 assignments; starts spread %.6fs with 2s transaction delay", spread)
	for p := 0; p < 4; p++ {
		eventually(t, "offset committed", func() bool { return f.offset(p) == 1 })
	}
}
func TestKafkaDuplicateDelivery(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "DUP")
	j := startJob(t, f.s, seg.ID, "30")
	b := batchesFor(t, f.s, j.ID)[0]
	f.consumers(t)
	f.publish(t)
	if err := queue.Send(context.Background(), f.writer, b); err != nil {
		t.Fatal(err)
	}
	f.completed(t, j)
	eventually(t, "both deliveries committed", func() bool { return f.offset(0) == 2 })
	if countRows(t, f.s, "processed_batches") != 1 || countRows(t, f.s, "user_segments") != 3 {
		t.Fatal("duplicate effects")
	}
}
func TestKafkaPoisonMessageQuarantined(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "POISON")
	j := startJob(t, f.s, seg.ID, "30")
	f.consumers(t)
	if err := f.writer.WriteMessages(context.Background(), kafka.Message{Partition: 0, Value: []byte("not json")}); err != nil {
		t.Fatal(err)
	}
	f.publish(t)
	f.completed(t, j)
	eventually(t, "poison and batch offsets", func() bool { return f.offset(0) == 2 })
	if countRows(t, f.s, "kafka_dead_letters") != 1 || countRows(t, f.s, "processed_batches") != 1 {
		t.Fatal("bad quarantine or lost batch")
	}
}
func TestKafkaOffsetWaitsForDatabaseCommit(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "OFFSET")
	j := startJob(t, f.s, seg.ID, "30")
	ctx := context.Background()
	holder, err := f.s.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_lock(934502)`); err != nil {
		t.Fatal(err)
	}
	defer holder.Exec(ctx, `SELECT pg_advisory_unlock(934502)`)
	if _, err = f.s.Pool.Exec(ctx, `CREATE FUNCTION gate_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(934502); RETURN NEW; END $$; CREATE TRIGGER gate_batch BEFORE INSERT ON processed_batches FOR EACH ROW EXECUTE FUNCTION gate_batch()`); err != nil {
		t.Fatal(err)
	}
	f.consumers(t)
	f.publish(t)
	eventually(t, "batch transaction blocked", func() bool {
		var b bool
		err := f.s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=934502 AND NOT granted)`).Scan(&b)
		return err == nil && b
	})
	if f.offset(0) > 0 || countRows(t, f.s, "processed_batches") != 0 || countRows(t, f.s, "user_segments") != 0 {
		t.Fatal("offset or effects visible before commit")
	}
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_unlock(934502)`); err != nil {
		t.Fatal(err)
	}
	f.completed(t, j)
	eventually(t, "committed offset after database commit", func() bool { return f.offset(0) == 1 })
}
func TestKafkaOutboxCommitFailureReplaysPublication(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "OUTBOX")
	j := startJob(t, f.s, seg.ID, "30")
	ctx := context.Background()
	if _, err := f.s.Pool.Exec(ctx, `CREATE SEQUENCE publish_attempt; CREATE FUNCTION fail_first_publish_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.published_at IS NOT NULL AND nextval('publish_attempt')=1 THEN RAISE EXCEPTION 'injected after Kafka acknowledgment'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_first_publish_mark BEFORE UPDATE ON outbox FOR EACH ROW EXECUTE FUNCTION fail_first_publish_mark()`); err != nil {
		t.Fatal(err)
	}
	found, err := f.s.DispatchOne(ctx, func(ctx context.Context, b domain.Batch) error { return queue.Send(ctx, f.writer, b) })
	if !found || err == nil {
		t.Fatal("expected publication marking failure")
	}
	var pending bool
	if err = f.s.Pool.QueryRow(ctx, `SELECT published_at IS NULL FROM outbox`).Scan(&pending); err != nil || !pending {
		t.Fatal("outbox was not rolled back")
	}
	f.publish(t)
	f.consumers(t)
	f.completed(t, j)
	eventually(t, "duplicate publication offsets", func() bool { return f.offset(0) == 2 })
	if countRows(t, f.s, "processed_batches") != 1 || countRows(t, f.s, "user_segments") != 3 {
		t.Fatal("duplicate publication effects")
	}
}
func TestKafkaConsumerDatabaseRetry(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "RETRY")
	j := startJob(t, f.s, seg.ID, "30")
	if _, err := f.s.Pool.Exec(context.Background(), `CREATE SEQUENCE batch_attempt; CREATE FUNCTION fail_first_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('batch_attempt')=1 THEN RAISE EXCEPTION 'temporary database failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_first_batch BEFORE INSERT ON processed_batches FOR EACH ROW EXECUTE FUNCTION fail_first_batch()`); err != nil {
		t.Fatal(err)
	}
	f.consumers(t)
	f.publish(t)
	f.completed(t, j)
	got, err := f.s.GetDistribution(context.Background(), j.ID)
	if err != nil || got.RetryCount != 1 || got.ProcessedBatches != 1 || countRows(t, f.s, "user_segments") != 3 {
		t.Fatalf("%+v %v", got, err)
	}
	eventually(t, "retry offset", func() bool { return f.offset(0) == 1 })
}
func TestKafkaPermanentFailureStatus(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "FAILED")
	j := startJob(t, f.s, seg.ID, "100")
	if _, err := f.s.Pool.Exec(context.Background(), `DELETE FROM users WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	f.consumers(t)
	f.publish(t)
	eventually(t, "failed job", func() bool {
		got, err := f.s.GetDistribution(context.Background(), j.ID)
		return err == nil && got.Status == "failed" && got.LastError != ""
	})
	eventually(t, "failed message offset", func() bool { return f.offset(0) == 1 })
	if countRows(t, f.s, "processed_batches") != 0 || countRows(t, f.s, "user_segments") != 0 {
		t.Fatal("partial failed batch committed")
	}
}
func TestKafkaTopicRequiresFourPartitions(t *testing.T) {
	f := kafkaSetup(t, 1)
	if err := queue.CheckTopic(context.Background(), f.w.Brokers, f.w.Topic); err == nil {
		t.Fatal("accepted insufficient partitions")
	}
}

func TestKafkaRestartAfterDatabaseCommitBeforeOffset(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "RESTART")
	j := startJob(t, f.s, seg.ID, "30")
	f.publish(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reader := f.w.NewReader()
	m, err := reader.FetchMessage(ctx)
	if err != nil {
		reader.Close()
		t.Fatal(err)
	}
	b, err := queue.Decode(m.Value)
	if err != nil {
		reader.Close()
		t.Fatal(err)
	}
	if _, err = f.s.ProcessBatch(ctx, b, domain.Delivery{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, ConsumerID: "before-restart"}); err != nil {
		reader.Close()
		t.Fatal(err)
	}
	if f.offset(0) > 0 {
		reader.Close()
		t.Fatal("unexpected commit")
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	f.consumers(t)
	f.completed(t, j)
	eventually(t, "replayed message committed after restart", func() bool { return f.offset(0) == 1 })
	if countRows(t, f.s, "processed_batches") != 1 || countRows(t, f.s, "user_segments") != 3 {
		t.Fatal("restart duplicated effects")
	}
}
func TestKafkaUnavailablePublisherRecovers(t *testing.T) {
	f := kafkaSetup(t, 4)
	seedUsers(t, f.s, 10)
	seg := createSegment(t, f.s, "OFFLINE")
	j := startJob(t, f.s, seg.ID, "30")
	bad := queue.NewWriter([]string{"127.0.0.1:1"}, f.w.Topic)
	defer bad.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	found, err := f.s.DispatchOne(ctx, func(ctx context.Context, b domain.Batch) error {
		send, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		return queue.Send(send, bad, b)
	})
	if !found || err == nil {
		t.Fatal("expected unavailable broker")
	}
	got, err := f.s.GetDistribution(context.Background(), j.ID)
	if err != nil || got.Status != "retrying" || got.RetryCount != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err = f.s.Pool.Exec(context.Background(), `UPDATE outbox SET available_at=now()`); err != nil {
		t.Fatal(err)
	}
	f.consumers(t)
	f.publish(t)
	f.completed(t, j)
	if countRows(t, f.s, "user_segments") != 3 {
		t.Fatal("job lost after publisher failure")
	}
}
