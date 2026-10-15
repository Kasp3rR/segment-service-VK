package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (store *Store) RecordRetry(ctx context.Context, batch domain.Batch, cause error) error {
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	_, err = store.Pool.Exec(ctx, `UPDATE distribution_jobs j SET status='retrying',last_error=$4,retry_count=retry_count+1 FROM outbox o
 WHERE j.id=$1 AND o.job_id=j.id AND o.batch_number=$2 AND o.payload=$3::jsonb AND j.status IN ('queued','running','retrying')`, batch.JobID, batch.Number, payload, "process: "+shortError(cause))
	return err
}

func (store *Store) FailJob(ctx context.Context, batch domain.Batch, cause error) error {
	tx, err := store.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var segmentID int64
	err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR UPDATE`, batch.SegmentID).Scan(&segmentID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE distribution_jobs j SET status='failed',last_error=$4,completed_at=clock_timestamp() FROM outbox o
 WHERE j.id=$1 AND o.job_id=j.id AND o.batch_number=$2 AND o.payload=$3::jsonb AND j.status IN ('queued','running','retrying')`, batch.JobID, batch.Number, payload, shortError(cause)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
