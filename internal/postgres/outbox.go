package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (store *Store) DispatchOne(ctx context.Context, publish func(context.Context, domain.Batch) error) (bool, error) {
	tx, err := store.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var id int64
	var payload []byte
	var attempts int
	err = tx.QueryRow(ctx, `SELECT o.id,o.payload,o.attempts FROM outbox o JOIN distribution_jobs j ON j.id=o.job_id
 WHERE o.published_at IS NULL AND o.available_at<=now() AND j.status IN ('queued','running','retrying')
 ORDER BY o.available_at,o.id FOR UPDATE OF o SKIP LOCKED LIMIT 1`).Scan(&id, &payload, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var batch domain.Batch
	if err = json.Unmarshal(payload, &batch); err != nil {
		return false, err
	}
	publishErr := publish(ctx, batch)
	if publishErr != nil {
		delay := time.Second * time.Duration(1<<min(attempts, 5))
		message := shortError(publishErr)
		if _, err = tx.Exec(ctx, `UPDATE outbox SET attempts=attempts+1,last_error=$2,available_at=clock_timestamp()+$3*interval '1 millisecond' WHERE id=$1`, id, message, delay.Milliseconds()); err != nil {
			return true, err
		}
		if _, err = tx.Exec(ctx, `UPDATE distribution_jobs SET status='retrying',last_error=$2,retry_count=retry_count+1 WHERE id=$1 AND status IN ('queued','running','retrying')`, batch.JobID, "publish: "+message); err != nil {
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

func saveOutboxBatches(ctx context.Context, tx pgx.Tx, batches []domain.Batch) error {
	const query = `INSERT INTO outbox(job_id, batch_number, payload) VALUES($1, $2, $3)`
	for _, batch := range batches {
		payload, err := json.Marshal(batch)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, query, batch.JobID, batch.Number, payload)
		if err != nil {
			return err
		}
	}
	return nil
}
