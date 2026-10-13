package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Kasp3rR/segment-service-VK/internal/distribution"
	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

const jobFields = `id::text,segment_id,segment_code,percent::text,snapshot_users,target_users,existing_users,planned_users,assigned_users,batch_count,processed_batches,status,last_error,retry_count,created_at,completed_at`

func scanJob(row pgx.Row) (domain.Job, error) {
	var j domain.Job
	var percent string
	err := row.Scan(&j.ID, &j.SegmentID, &j.SegmentCode, &percent, &j.SnapshotUsers, &j.TargetUsers, &j.ExistingUsers, &j.PlannedUsers, &j.AssignedUsers, &j.BatchCount, &j.ProcessedBatches, &j.Status, &j.LastError, &j.RetryCount, &j.CreatedAt, &j.CompletedAt)
	j.Percent = json.Number(percent)
	return j, mapError(err)
}
func ensureNoActive(ctx context.Context, tx pgx.Tx, segmentID int64) error {
	var active bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM distribution_jobs WHERE segment_id=$1 AND status IN ('queued','running','retrying'))`, segmentID).Scan(&active)
	if err != nil {
		return err
	}
	if active {
		return fmt.Errorf("%w: segment has an active distribution", domain.ErrConflict)
	}
	return nil
}
func (s *Store) StartDistribution(ctx context.Context, segmentID int64, percent string) (domain.Job, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback(ctx)
	var code string
	if err = tx.QueryRow(ctx, `SELECT code FROM segments WHERE id=$1 FOR UPDATE`, segmentID).Scan(&code); err != nil {
		return domain.Job{}, mapError(err)
	}
	if err = ensureNoActive(ctx, tx, segmentID); err != nil {
		return domain.Job{}, err
	}
	rows, err := tx.Query(ctx, `SELECT u.id,us.user_id IS NOT NULL FROM users u LEFT JOIN user_segments us ON us.user_id=u.id AND us.segment_id=$1`, segmentID)
	if err != nil {
		return domain.Job{}, err
	}
	users := make([]domain.SnapshotUser, 0)
	for rows.Next() {
		var u domain.SnapshotUser
		if err = rows.Scan(&u.ID, &u.Assigned); err != nil {
			rows.Close()
			return domain.Job{}, err
		}
		users = append(users, u)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return domain.Job{}, err
	}
	plan, err := distribution.BuildPlan(segmentID, percent, users)
	if err != nil {
		return domain.Job{}, err
	}
	count := (len(plan.UserIDs) + domain.BatchSize - 1) / domain.BatchSize
	status := "queued"
	if count == 0 {
		status = "completed"
	}
	job, err := scanJob(tx.QueryRow(ctx, `INSERT INTO distribution_jobs(segment_id,segment_code,percent,snapshot_users,target_users,existing_users,planned_users,batch_count,status,completed_at)
 VALUES($1,$2,$3::numeric,$4,$5,$6,$7,$8,$9,CASE WHEN $9='completed' THEN clock_timestamp() ELSE NULL END) RETURNING `+jobFields, segmentID, code, percent, len(users), plan.Target, plan.Existing, len(plan.UserIDs), count, status))
	if err != nil {
		return domain.Job{}, err
	}
	for _, b := range distribution.Batches(job.ID, segmentID, plan.UserIDs) {
		raw, err := json.Marshal(b)
		if err != nil {
			return domain.Job{}, err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO outbox(job_id,batch_number,payload) VALUES($1,$2,$3)`, b.JobID, b.Number, raw); err != nil {
			return domain.Job{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}
func (s *Store) GetDistribution(ctx context.Context, id string) (domain.Job, error) {
	return scanJob(s.Pool.QueryRow(ctx, `SELECT `+jobFields+` FROM distribution_jobs WHERE id=$1`, id))
}
func (s *Store) DispatchOne(ctx context.Context, publish func(context.Context, domain.Batch) error) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id int64
	var raw []byte
	var attempts int
	err = tx.QueryRow(ctx, `SELECT o.id,o.payload,o.attempts FROM outbox o JOIN distribution_jobs j ON j.id=o.job_id
 WHERE o.published_at IS NULL AND o.available_at<=now() AND j.status IN ('queued','running','retrying')
 ORDER BY o.available_at,o.id FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&id, &raw, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var b domain.Batch
	if err = json.Unmarshal(raw, &b); err != nil {
		return false, err
	}
	publishErr := publish(ctx, b)
	if publishErr != nil {
		delay := time.Second * time.Duration(1<<min(attempts, 5))
		message := shortError(publishErr)
		if _, err = tx.Exec(ctx, `UPDATE outbox SET attempts=attempts+1,last_error=$2,available_at=clock_timestamp()+$3*interval '1 millisecond' WHERE id=$1`, id, message, delay.Milliseconds()); err != nil {
			return true, err
		}
		if _, err = tx.Exec(ctx, `UPDATE distribution_jobs SET status='retrying',last_error=$2,retry_count=retry_count+1 WHERE id=$1 AND status IN ('queued','running','retrying')`, b.JobID, "publish: "+message); err != nil {
			return true, err
		}
	} else {
		if _, err = tx.Exec(ctx, `UPDATE outbox SET attempts=attempts+1,published_at=clock_timestamp(),last_error='' WHERE id=$1`, id); err != nil {
			return true, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return true, err
	}
	return true, publishErr
}
func shortError(err error) string {
	s := []rune(err.Error())
	if len(s) > 2000 {
		s = s[:2000]
	}
	return string(s)
}
func (s *Store) ProcessBatch(ctx context.Context, b domain.Batch, d domain.Delivery) (bool, error) {
	if err := distribution.ValidateBatch(b); err != nil {
		return false, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	raw, err := json.Marshal(b)
	if err != nil {
		return false, err
	}
	var matches bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox WHERE job_id=$1 AND batch_number=$2 AND payload=$3::jsonb)`, b.JobID, b.Number, raw).Scan(&matches); err != nil {
		return false, err
	}
	if !matches {
		return false, domain.ErrInvalidMessage
	}
	var status string
	if err = tx.QueryRow(ctx, `SELECT status FROM distribution_jobs WHERE id=$1`, b.JobID).Scan(&status); err != nil {
		return false, err
	}
	if status == "completed" || status == "failed" {
		return false, tx.Commit(ctx)
	}
	var sid int64
	if err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR KEY SHARE`, b.SegmentID).Scan(&sid); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("%w: segment no longer exists", domain.ErrBatchFailed)
		}
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT status FROM distribution_jobs WHERE id=$1`, b.JobID).Scan(&status); err != nil {
		return false, err
	}
	if status == "completed" || status == "failed" {
		return false, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `INSERT INTO processed_batches(job_id,batch_number,consumer_id,kafka_partition,kafka_offset) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, b.JobID, b.Number, d.ConsumerID, d.Partition, d.Offset)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	tag, err = tx.Exec(ctx, `INSERT INTO user_segments(user_id,segment_id) SELECT unnest($1::bigint[]),$2 ON CONFLICT (user_id,segment_id) DO NOTHING`, b.UserIDs, b.SegmentID)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && (pgerr.Code == "23503" || pgerr.Code == "23514") {
			return false, fmt.Errorf("%w: assignment violates database constraints", domain.ErrBatchFailed)
		}
		return false, err
	}
	inserted := tag.RowsAffected()
	var processed, total int
	var target int64
	if err = tx.QueryRow(ctx, `SELECT processed_batches,batch_count,target_users FROM distribution_jobs WHERE id=$1 FOR UPDATE`, b.JobID).Scan(&processed, &total, &target); err != nil {
		return false, err
	}
	if processed+1 == total {
		var actual int64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM user_segments WHERE segment_id=$1`, b.SegmentID).Scan(&actual); err != nil {
			return false, err
		}
		if actual != target {
			return false, fmt.Errorf("%w: final segment size %d differs from target %d", domain.ErrBatchFailed, actual, target)
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE processed_batches SET inserted_users=$3 WHERE job_id=$1 AND batch_number=$2`, b.JobID, b.Number, inserted); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE distribution_jobs SET processed_batches=processed_batches+1,assigned_users=assigned_users+$2,
 status=CASE WHEN processed_batches+1=batch_count THEN 'completed' ELSE 'running' END,
 last_error=CASE WHEN processed_batches+1=batch_count THEN '' ELSE last_error END,
 completed_at=CASE WHEN processed_batches+1=batch_count THEN clock_timestamp() ELSE NULL END WHERE id=$1`, b.JobID, inserted); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
func (s *Store) RecordRetry(ctx context.Context, b domain.Batch, cause error) error {
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	_, err = s.Pool.Exec(ctx, `UPDATE distribution_jobs j SET status='retrying',last_error=$4,retry_count=retry_count+1 FROM outbox o
 WHERE j.id=$1 AND o.job_id=j.id AND o.batch_number=$2 AND o.payload=$3::jsonb AND j.status IN ('queued','running','retrying')`, b.JobID, b.Number, raw, "process: "+shortError(cause))
	return err
}
func (s *Store) FailJob(ctx context.Context, b domain.Batch, cause error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var sid int64
	err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR UPDATE`, b.SegmentID).Scan(&sid)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE distribution_jobs j SET status='failed',last_error=$4,completed_at=clock_timestamp() FROM outbox o
 WHERE j.id=$1 AND o.job_id=j.id AND o.batch_number=$2 AND o.payload=$3::jsonb AND j.status IN ('queued','running','retrying')`, b.JobID, b.Number, raw, shortError(cause)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (s *Store) Quarantine(ctx context.Context, d domain.Delivery, payload []byte, cause error) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO kafka_dead_letters(topic,partition,message_offset,payload,reason) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, d.Topic, d.Partition, d.Offset, payload, shortError(cause)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
