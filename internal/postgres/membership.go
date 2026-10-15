package postgres

import (
	"context"
	"time"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Store) SetMembership(ctx context.Context, userID, segmentID int64, add bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR UPDATE`, segmentID).Scan(&found); err != nil {
		return mapError(err)
	}
	if err = ensureNoActive(ctx, tx, segmentID); err != nil {
		return err
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR KEY SHARE`, userID).Scan(&found); err != nil {
		return mapError(err)
	}
	if add {
		_, err = tx.Exec(ctx, `INSERT INTO user_segments(user_id,segment_id) VALUES($1,$2) ON CONFLICT (user_id,segment_id) DO NOTHING`, userID, segmentID)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM user_segments WHERE user_id=$1 AND segment_id=$2`, userID, segmentID)
	}
	if err != nil {
		return mapError(err)
	}
	return mapError(tx.Commit(ctx))
}

func (s *Store) UserSegments(ctx context.Context, userID int64) ([]domain.Segment, error) {
	const query = `SELECT s.id,s.code,s.name,s.description,s.created_at
 FROM users u LEFT JOIN user_segments us ON us.user_id=u.id
 LEFT JOIN segments s ON s.id=us.segment_id WHERE u.id=$1 ORDER BY us.segment_id`

	rows, err := s.Pool.Query(ctx, query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	segments := []domain.Segment{}
	exists := false
	for rows.Next() {
		exists = true
		var id *int64
		var code, name, description *string
		var segment domain.Segment
		var timestamp *time.Time
		if err = rows.Scan(&id, &code, &name, &description, &timestamp); err != nil {
			return nil, err
		}
		if id != nil {
			segment = domain.Segment{
				ID:          *id,
				Code:        *code,
				Name:        *name,
				Description: *description,
				CreatedAt:   *timestamp,
			}
			segments = append(segments, segment)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.ErrNotFound
	}
	return segments, nil
}
