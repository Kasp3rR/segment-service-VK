package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/Kasp3rR/segment-service-VK/internal/distribution"
	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

const jobFields = `id::text,segment_id,segment_code,percent::text,snapshot_users,target_users,existing_users,planned_users,assigned_users,batch_count,processed_batches,status,last_error,retry_count,created_at,completed_at`

func scanJob(row pgx.Row) (domain.Job, error) {
	var job domain.Job
	var percent string
	err := row.Scan(&job.ID, &job.SegmentID, &job.SegmentCode, &percent, &job.SnapshotUsers, &job.TargetUsers, &job.ExistingUsers, &job.PlannedUsers, &job.AssignedUsers, &job.BatchCount, &job.ProcessedBatches, &job.Status, &job.LastError, &job.RetryCount, &job.CreatedAt, &job.CompletedAt)
	job.Percent = json.Number(percent)
	return job, mapError(err)
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

func (store *Store) StartDistribution(ctx context.Context, segmentID int64, percent string) (domain.Job, error) {
	tx, err := store.Pool.Begin(ctx)
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
	users, err := loadUserSnapshot(ctx, tx, segmentID)
	if err != nil {
		return domain.Job{}, err
	}
	plan, err := distribution.BuildPlan(segmentID, percent, users)
	if err != nil {
		return domain.Job{}, err
	}
	batchCount := (len(plan.UserIDs) + domain.BatchSize - 1) / domain.BatchSize
	status := "queued"
	if batchCount == 0 {
		status = "completed"
	}
	job, err := scanJob(tx.QueryRow(ctx, `INSERT INTO distribution_jobs(segment_id,segment_code,percent,snapshot_users,target_users,existing_users,planned_users,batch_count,status,completed_at)
 VALUES($1,$2,$3::numeric,$4,$5,$6,$7,$8,$9,CASE WHEN $9='completed' THEN clock_timestamp() ELSE NULL END) RETURNING `+jobFields, segmentID, code, percent, len(users), plan.Target, plan.Existing, len(plan.UserIDs), batchCount, status))
	if err != nil {
		return domain.Job{}, err
	}
	batches := distribution.Batches(job.ID, segmentID, plan.UserIDs)
	err = saveOutboxBatches(ctx, tx, batches)
	if err != nil {
		return domain.Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func (store *Store) GetDistribution(ctx context.Context, id string) (domain.Job, error) {
	return scanJob(store.Pool.QueryRow(ctx, `SELECT `+jobFields+` FROM distribution_jobs WHERE id=$1`, id))
}

func loadUserSnapshot(ctx context.Context, tx pgx.Tx, segmentID int64) ([]domain.SnapshotUser, error) {
	const query = `
  SELECT u.id, us.user_id IS NOT NULL
  FROM users u
  LEFT JOIN user_segments us ON us.user_id = u.id AND us.segment_id = $1
 `
	rows, err := tx.Query(ctx, query, segmentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	users := make([]domain.SnapshotUser, 0)
	for rows.Next() {
		var user domain.SnapshotUser
		err = rows.Scan(&user.ID, &user.Assigned)
		if err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}
