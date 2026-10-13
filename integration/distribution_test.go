//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
	"github.com/Kasp3rR/segment-service-VK/internal/httpapi"
	"github.com/Kasp3rR/segment-service-VK/internal/postgres"
	"github.com/Kasp3rR/segment-service-VK/internal/service"
)

func seedUsers(t *testing.T, s *postgres.Store, count int) {
	t.Helper()
	if _, err := s.Pool.Exec(context.Background(), `INSERT INTO users(id) SELECT generate_series(1,$1::bigint)`, count); err != nil {
		t.Fatal(err)
	}
}
func batchesFor(t *testing.T, s *postgres.Store, job string) []domain.Batch {
	t.Helper()
	rows, err := s.Pool.Query(context.Background(), `SELECT payload FROM outbox WHERE job_id=$1 ORDER BY batch_number`, job)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := []domain.Batch{}
	for rows.Next() {
		var raw []byte
		var b domain.Batch
		if err = rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		out = append(out, b)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
func startJob(t *testing.T, s *postgres.Store, sid int64, p string) domain.Job {
	t.Helper()
	j, err := s.StartDistribution(context.Background(), sid, p)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func processAll(t *testing.T, s *postgres.Store, j domain.Job) {
	t.Helper()
	for _, b := range batchesFor(t, s, j.ID) {
		if _, err := s.ProcessBatch(context.Background(), b, domain.Delivery{ConsumerID: "test", Partition: b.Number % 4}); err != nil {
			t.Fatal(err)
		}
	}
}
func countRows(t *testing.T, s *postgres.Store, table string) int64 {
	t.Helper()
	var n int64
	allowed := map[string]bool{"users": true, "user_segments": true, "outbox": true, "processed_batches": true, "distribution_jobs": true, "kafka_dead_letters": true}
	if !allowed[table] {
		t.Fatal("invalid table")
	}
	if err := s.Pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func TestDistributionSnapshotAndRounding(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 7)
	seg := createSegment(t, s, "MAIL_GPT")
	if err := s.SetMembership(context.Background(), 1, seg.ID, true); err != nil {
		t.Fatal(err)
	}
	j := startJob(t, s, seg.ID, "30")
	if j.SnapshotUsers != 7 || j.TargetUsers != 2 || j.ExistingUsers != 1 || j.PlannedUsers != 1 || j.Status != "queued" {
		t.Fatalf("%+v", j)
	}
	createUser(t, s, 999)
	processAll(t, s, j)
	got, err := s.GetDistribution(context.Background(), j.ID)
	if err != nil || got.Status != "completed" || got.AssignedUsers != 1 || got.CompletedAt == nil || countRows(t, s, "user_segments") != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	list, err := s.UserSegments(context.Background(), 999)
	if err != nil || len(list) != 0 {
		t.Fatal("new user entered old snapshot")
	}
}
func TestDistributionZeroPopulation(t *testing.T) {
	s := fixture(t, true)
	seg := createSegment(t, s, "EMPTY")
	j := startJob(t, s, seg.ID, "30")
	if j.Status != "completed" || j.BatchCount != 0 || j.CompletedAt == nil || countRows(t, s, "outbox") != 0 {
		t.Fatalf("%+v", j)
	}
}
func TestDistributionPercentEdges(t *testing.T) {
	for _, p := range []string{"0", "100", "0.29"} {
		t.Run(p, func(t *testing.T) {
			s := fixture(t, true)
			seedUsers(t, s, 10000)
			seg := createSegment(t, s, "EDGE")
			j := startJob(t, s, seg.ID, p)
			processAll(t, s, j)
			want := map[string]int64{"0": 0, "100": 10000, "0.29": 29}[p]
			if n := countRows(t, s, "user_segments"); n != want {
				t.Fatalf("got %d want %d", n, want)
			}
		})
	}
}
func TestDistributionExistingAssignmentsConflict(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "CONFLICT")
	for id := int64(1); id <= 3; id++ {
		if err := s.SetMembership(context.Background(), id, seg.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.StartDistribution(context.Background(), seg.ID, "20"); !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
	if countRows(t, s, "distribution_jobs") != 0 || countRows(t, s, "outbox") != 0 {
		t.Fatal("conflict persisted a job")
	}
}
func TestDistributionActiveGuards(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "ACTIVE")
	other := createSegment(t, s, "OTHER")
	startJob(t, s, seg.ID, "30")
	ctx := context.Background()
	for _, call := range []func() error{func() error { return s.SetMembership(ctx, 1, seg.ID, true) }, func() error { return s.SetMembership(ctx, 1, seg.ID, false) }, func() error { return s.DeleteSegment(ctx, seg.ID) }, func() error { _, e := s.StartDistribution(ctx, seg.ID, "50"); return e }} {
		if err := call(); !errors.Is(err, domain.ErrConflict) {
			t.Fatal(err)
		}
	}
	createUser(t, s, 100)
	if err := s.SetMembership(ctx, 1, other.ID, true); err != nil {
		t.Fatal(err)
	}
}
func TestDistributionDeterministicDatabaseSelection(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 100)
	seg := createSegment(t, s, "SAME")
	first := startJob(t, s, seg.ID, "30")
	a := batchesFor(t, s, first.ID)[0].UserIDs
	processAll(t, s, first)
	for _, uid := range a {
		if err := s.SetMembership(context.Background(), uid, seg.ID, false); err != nil {
			t.Fatal(err)
		}
	}
	second := startJob(t, s, seg.ID, "30")
	b := batchesFor(t, s, second.ID)[0].UserIDs
	if !reflect.DeepEqual(a, b) {
		t.Fatal("selection is not reproducible")
	}
}
func TestDistributionPersistedBatchBoundaries(t *testing.T) {
	for _, n := range []int{4999, 5000, 5001, 10000, 10001} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			s := fixture(t, true)
			seedUsers(t, s, n)
			seg := createSegment(t, s, "BATCHES")
			j := startJob(t, s, seg.ID, "100")
			batches := batchesFor(t, s, j.ID)
			if len(batches) != (n+4999)/5000 {
				t.Fatal(len(batches))
			}
			seen := map[int64]bool{}
			for i, b := range batches {
				if b.Number != i || len(b.UserIDs) > 5000 || len(b.UserIDs) == 0 {
					t.Fatal("invalid persisted batch")
				}
				for _, uid := range b.UserIDs {
					if seen[uid] {
						t.Fatal("duplicate candidate")
					}
					seen[uid] = true
				}
			}
			if len(seen) != n {
				t.Fatal("missing candidate")
			}
		})
	}
}
func TestDistributionDuplicateConcurrentBatch(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 5001)
	seg := createSegment(t, s, "DUPLICATE")
	j := startJob(t, s, seg.ID, "100")
	b := batchesFor(t, s, j.ID)
	errorsCh := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, e := s.ProcessBatch(context.Background(), b[0], domain.Delivery{ConsumerID: "test"})
			errorsCh <- e
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.GetDistribution(context.Background(), j.ID)
	if err != nil || got.ProcessedBatches != 1 || got.AssignedUsers != 5000 || countRows(t, s, "processed_batches") != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	processAll(t, s, j)
	processAll(t, s, j)
	if countRows(t, s, "user_segments") != 5001 || countRows(t, s, "processed_batches") != 2 {
		t.Fatal("duplicate effects")
	}
}
func TestDistributionBatchTransactionRollback(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "ROLLBACK")
	j := startJob(t, s, seg.ID, "100")
	b := batchesFor(t, s, j.ID)[0]
	ctx := context.Background()
	_, err := s.Pool.Exec(ctx, `CREATE FUNCTION reject_progress() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected progress failure'; END $$; CREATE TRIGGER reject_progress BEFORE UPDATE ON distribution_jobs FOR EACH ROW EXECUTE FUNCTION reject_progress()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ProcessBatch(ctx, b, domain.Delivery{}); err == nil {
		t.Fatal("expected injected failure")
	}
	if countRows(t, s, "user_segments") != 0 || countRows(t, s, "processed_batches") != 0 {
		t.Fatal("partial transaction committed")
	}
	if _, err = s.Pool.Exec(ctx, `DROP TRIGGER reject_progress ON distribution_jobs`); err != nil {
		t.Fatal(err)
	}
	processAll(t, s, j)
}
func TestDistributionOutboxAtomicCreation(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "OUTBOX")
	ctx := context.Background()
	if _, err := s.Pool.Exec(ctx, `CREATE FUNCTION reject_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox failure'; END $$; CREATE TRIGGER reject_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_outbox()`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartDistribution(ctx, seg.ID, "100"); err == nil {
		t.Fatal("expected injected failure")
	}
	if countRows(t, s, "distribution_jobs") != 0 || countRows(t, s, "outbox") != 0 {
		t.Fatal("job committed without its messages")
	}
}
func TestDistributionOutboxRetryState(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "RETRY")
	j := startJob(t, s, seg.ID, "100")
	ctx := context.Background()
	offline := errors.New("broker unavailable")
	found, err := s.DispatchOne(ctx, func(context.Context, domain.Batch) error { return offline })
	if !found || !errors.Is(err, offline) {
		t.Fatal(err)
	}
	got, err := s.GetDistribution(ctx, j.ID)
	if err != nil || got.Status != "retrying" || got.RetryCount != 1 || got.LastError == "" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE outbox SET available_at=now()`); err != nil {
		t.Fatal(err)
	}
	found, err = s.DispatchOne(ctx, func(context.Context, domain.Batch) error { return nil })
	if !found || err != nil {
		t.Fatal(err)
	}
	found, err = s.DispatchOne(ctx, func(context.Context, domain.Batch) error { t.Fatal("published message sent twice"); return nil })
	if found || err != nil {
		t.Fatal(err)
	}
	if countRows(t, s, "user_segments") != 0 {
		t.Fatal("publisher assigned users directly")
	}
}
func TestDistributionConcurrentOutboxClaims(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 20000)
	seg := createSegment(t, s, "CLAIMS")
	startJob(t, s, seg.ID, "100")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	claimed := make(chan int, 4)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			_, e := s.DispatchOne(ctx, func(_ context.Context, b domain.Batch) error {
				claimed <- b.Number
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			done <- e
		}()
	}
	seen := map[int]bool{}
	for i := 0; i < 4; i++ {
		select {
		case n := <-claimed:
			if seen[n] {
				t.Fatal("same outbox row claimed twice")
			}
			seen[n] = true
		case <-ctx.Done():
			t.Fatal("claims serialized")
		}
	}
	once.Do(func() { close(release) })
	for i := 0; i < 4; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
func TestDistributionPermanentFailure(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 5001)
	seg := createSegment(t, s, "FAILED")
	j := startJob(t, s, seg.ID, "100")
	b := batchesFor(t, s, j.ID)
	ctx := context.Background()
	if _, err := s.ProcessBatch(ctx, b[0], domain.Delivery{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `DELETE FROM users WHERE id=$1`, b[1].UserIDs[0]); err != nil {
		t.Fatal(err)
	}
	_, err := s.ProcessBatch(ctx, b[1], domain.Delivery{})
	if !errors.Is(err, domain.ErrBatchFailed) {
		t.Fatal(err)
	}
	if err = s.FailJob(ctx, b[1], err); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDistribution(ctx, j.ID)
	if err != nil || got.Status != "failed" || got.AssignedUsers != 5000 || got.ProcessedBatches != 1 || got.LastError == "" {
		t.Fatalf("%+v %v", got, err)
	}
	if applied, err := s.ProcessBatch(ctx, b[1], domain.Delivery{}); err != nil || applied {
		t.Fatal("failed job continued")
	}
	if err = s.DeleteSegment(ctx, seg.ID); err != nil {
		t.Fatal(err)
	}
}
func TestDistributionTamperedPayloadRejected(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "TAMPER")
	j := startJob(t, s, seg.ID, "30")
	b := batchesFor(t, s, j.ID)[0]
	b.UserIDs = []int64{1}
	if _, err := s.ProcessBatch(context.Background(), b, domain.Delivery{}); !errors.Is(err, domain.ErrInvalidMessage) {
		t.Fatal(err)
	}
	if err := s.RecordRetry(context.Background(), b, errors.New("forged")); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDistribution(context.Background(), j.ID)
	if err != nil || got.Status != "queued" || got.RetryCount != 0 || countRows(t, s, "user_segments") != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}
func TestDistributionHistoryAfterSegmentDeletion(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "HISTORY")
	j := startJob(t, s, seg.ID, "30")
	b := batchesFor(t, s, j.ID)[0]
	processAll(t, s, j)
	if err := s.DeleteSegment(context.Background(), seg.ID); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetDistribution(context.Background(), j.ID)
	if err != nil || got.SegmentID != seg.ID || got.SegmentCode != "HISTORY" || got.Status != "completed" {
		t.Fatalf("%+v %v", got, err)
	}
	if applied, err := s.ProcessBatch(context.Background(), b, domain.Delivery{}); err != nil || applied {
		t.Fatal("completed replay failed after deletion")
	}
}
func TestDistributionConcurrentStarts(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "STARTS")
	start := make(chan struct{})
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; _, e := s.StartDistribution(context.Background(), seg.ID, "30"); done <- e }()
	}
	close(start)
	success, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		err := <-done
		if err == nil {
			success++
		} else if errors.Is(err, domain.ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 || countRows(t, s, "distribution_jobs") != 1 {
		t.Fatal("concurrent starts created two active jobs")
	}
}
func TestDistributionSnapshotDuringUserCreation(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 10)
	seg := createSegment(t, s, "SNAPSHOT")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	holder, err := s.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Release()
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_lock(934501)`); err != nil {
		t.Fatal(err)
	}
	defer holder.Exec(context.Background(), `SELECT pg_advisory_unlock(934501)`)
	if _, err = s.Pool.Exec(ctx, `CREATE FUNCTION pause_outbox() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(934501); RETURN NEW; END $$; CREATE TRIGGER pause_outbox BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION pause_outbox()`); err != nil {
		t.Fatal(err)
	}
	type result struct {
		j domain.Job
		e error
	}
	done := make(chan result, 1)
	go func() { j, e := s.StartDistribution(ctx, seg.ID, "30"); done <- result{j, e} }()
	for {
		var blocked bool
		if err = s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=934501 AND NOT granted)`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	createUser(t, s, 999)
	if _, err = holder.Exec(ctx, `SELECT pg_advisory_unlock(934501)`); err != nil {
		t.Fatal(err)
	}
	r := <-done
	if r.e != nil || r.j.SnapshotUsers != 10 || r.j.TargetUsers != 3 {
		t.Fatalf("%+v %v", r.j, r.e)
	}
	for _, b := range batchesFor(t, s, r.j.ID) {
		for _, uid := range b.UserIDs {
			if uid == 999 {
				t.Fatal("new user leaked into old snapshot")
			}
		}
	}
}
func TestDistributionHTTP(t *testing.T) {
	s := fixture(t, true)
	seedUsers(t, s, 7)
	seg := createSegment(t, s, "HTTP")
	h := httpapi.New(service.New(s), s.Pool.Ping, zap.NewNop(), time.Second)
	path := fmt.Sprintf("/segments/%d/distributions", seg.ID)
	for _, body := range []string{`{}`, `null`, `{"percent":null}`, `{"percent":"30"}`, `{"percent":101}`, `{"percent":-1}`, `{"percent":30,"extra":true}`} {
		request(t, h, "POST", path, body, 400)
	}
	raw := request(t, h, "POST", path, `{"percent":30}`, 202)
	var j domain.Job
	if err := json.Unmarshal(raw, &j); err != nil || j.TargetUsers != 2 {
		t.Fatalf("%s %v", raw, err)
	}
	request(t, h, "GET", "/distributions/"+j.ID, "", 200)
	request(t, h, "GET", "/distributions/invalid", "", 400)
	request(t, h, "GET", "/distributions/11111111-2222-4333-8444-555555555555", "", 404)
	request(t, h, "POST", path, `{"percent":30}`, 409)
}
func TestDistributionQuarantineIdempotency(t *testing.T) {
	s := fixture(t, true)
	d := domain.Delivery{Topic: "bad", Partition: 1, Offset: 3}
	for i := 0; i < 2; i++ {
		if err := s.Quarantine(context.Background(), d, []byte("broken"), domain.ErrInvalidMessage); err != nil {
			t.Fatal(err)
		}
	}
	if countRows(t, s, "kafka_dead_letters") != 1 {
		t.Fatal("duplicate dead letter")
	}
}
