package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func scanSegment(row pgx.Row) (domain.Segment, error) {
	var s domain.Segment
	err := row.Scan(&s.ID, &s.Code, &s.Name, &s.Description, &s.CreatedAt)
	return s, mapError(err)
}

func (s *Store) CreateSegment(ctx context.Context, request domain.CreateSegment) (domain.Segment, error) {
	const query = `INSERT INTO segments(code,name,description) VALUES($1,$2,$3) RETURNING id,code,name,description,created_at`

	row := s.Pool.QueryRow(ctx, query, request.Code, request.Name, request.Description)
	return scanSegment(row)
}

func (s *Store) GetSegment(ctx context.Context, id int64) (domain.Segment, error) {
	const query = `SELECT id,code,name,description,created_at FROM segments WHERE id=$1`

	row := s.Pool.QueryRow(ctx, query, id)
	return scanSegment(row)
}

func (s *Store) ListSegments(ctx context.Context, request domain.Page) ([]domain.Segment, error) {
	const query = `SELECT id,code,name,description,created_at FROM segments WHERE id>$1 ORDER BY id LIMIT $2`

	rows, err := s.Pool.Query(ctx, query, request.AfterID, request.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	segments := []domain.Segment{}
	for rows.Next() {
		segment, err := scanSegment(rows)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment)
	}
	return segments, rows.Err()
}

func (s *Store) PatchSegment(ctx context.Context, id int64, request domain.PatchSegment) (domain.Segment, error) {
	const query = `UPDATE segments SET name=COALESCE($2,name),description=COALESCE($3,description) WHERE id=$1 RETURNING id,code,name,description,created_at`

	row := s.Pool.QueryRow(ctx, query, id, request.Name, request.Description)
	return scanSegment(row)
}

func (s *Store) DeleteSegment(ctx context.Context, id int64) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR UPDATE`, id).Scan(&found); err != nil {
		return mapError(err)
	}
	if err = ensureNoActive(ctx, tx, id); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM segments WHERE id=$1`, id); err != nil {
		return mapError(err)
	}
	return mapError(tx.Commit(ctx))
}
