package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Kasp3rR/segment-service-VK/internal/distribution"
	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (store *Store) ProcessBatch(ctx context.Context, batch domain.Batch, delivery domain.Delivery) (bool, error) {
	if err := distribution.ValidateBatch(batch); err != nil {
		return false, err
	}
	tx, err := store.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	payload, err := json.Marshal(batch)
	if err != nil {
		return false, err
	}
	var matches bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM outbox WHERE job_id=$1 AND batch_number=$2 AND payload=$3::jsonb)`, batch.JobID, batch.Number, payload).Scan(&matches); err != nil {
		return false, err
	}
	if !matches {
		return false, domain.ErrInvalidMessage
	}
	var status string
	if err = tx.QueryRow(ctx, `SELECT status FROM distribution_jobs WHERE id=$1`, batch.JobID).Scan(&status); err != nil {
		return false, err
	}
	if status == "completed" || status == "failed" {
		return false, tx.Commit(ctx)
	}
	var segmentID int64
	if err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR KEY SHARE`, batch.SegmentID).Scan(&segmentID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("%w: segment no longer exists", domain.ErrBatchFailed)
		}
		return false, err
	}
	if err = tx.QueryRow(ctx, `SELECT status FROM distribution_jobs WHERE id=$1`, batch.JobID).Scan(&status); err != nil {
		return false, err
	}
	if status == "completed" || status == "failed" {
		return false, tx.Commit(ctx)
	}
	result, err := tx.Exec(ctx, `INSERT INTO processed_batches(job_id,batch_number,consumer_id,kafka_partition,kafka_offset) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, batch.JobID, batch.Number, delivery.ConsumerID, delivery.Partition, delivery.Offset)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	result, err = tx.Exec(ctx, `INSERT INTO user_segments(user_id,segment_id) SELECT unnest($1::bigint[]),$2 ON CONFLICT (user_id,segment_id) DO NOTHING`, batch.UserIDs, batch.SegmentID)
	if err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && (pgerr.Code == "23503" || pgerr.Code == "23514") {
			return false, fmt.Errorf("%w: assignment violates database constraints", domain.ErrBatchFailed)
		}
		return false, err
	}
	inserted := result.RowsAffected()
	var processedBatches, batchCount int
	var target int64
	if err = tx.QueryRow(ctx, `SELECT processed_batches,batch_count,target_users FROM distribution_jobs WHERE id=$1 FOR UPDATE`, batch.JobID).Scan(&processedBatches, &batchCount, &target); err != nil {
		return false, err
	}
	if processedBatches+1 == batchCount {
		var actual int64
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM user_segments WHERE segment_id=$1`, batch.SegmentID).Scan(&actual); err != nil {
			return false, err
		}
		if actual != target {
			return false, fmt.Errorf("%w: final segment size %d differs from target %d", domain.ErrBatchFailed, actual, target)
		}
	}
	if _, err = tx.Exec(ctx, `UPDATE processed_batches SET inserted_users=$3 WHERE job_id=$1 AND batch_number=$2`, batch.JobID, batch.Number, inserted); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE distribution_jobs SET processed_batches=processed_batches+1,assigned_users=assigned_users+$2,
 status=CASE WHEN processed_batches+1=batch_count THEN 'completed' ELSE 'running' END,
 last_error=CASE WHEN processed_batches+1=batch_count THEN '' ELSE last_error END,
 completed_at=CASE WHEN processed_batches+1=batch_count THEN clock_timestamp() ELSE NULL END WHERE id=$1`, batch.JobID, inserted); err != nil {
		return false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
