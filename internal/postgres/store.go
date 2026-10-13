package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string, max int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = max
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Store{Pool: p}, nil
}
func mapError(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	var e *pgconn.PgError
	if errors.As(err, &e) {
		switch e.Code {
		case "23505":
			return domain.ErrConflict
		case "23503":
			return domain.ErrNotFound
		case "23514", "22001":
			return domain.ErrInvalid
		}
	}
	return err
}
func scanSegment(row pgx.Row) (domain.Segment, error) {
	var s domain.Segment
	err := row.Scan(&s.ID, &s.Code, &s.Name, &s.Description, &s.CreatedAt)
	return s, mapError(err)
}
func (s *Store) CreateSegment(ctx context.Context, in domain.CreateSegment) (domain.Segment, error) {
	return scanSegment(s.Pool.QueryRow(ctx, `INSERT INTO segments(code,name,description) VALUES($1,$2,$3) RETURNING id,code,name,description,created_at`, in.Code, in.Name, in.Description))
}
func (s *Store) GetSegment(ctx context.Context, id int64) (domain.Segment, error) {
	return scanSegment(s.Pool.QueryRow(ctx, `SELECT id,code,name,description,created_at FROM segments WHERE id=$1`, id))
}
func (s *Store) ListSegments(ctx context.Context, p domain.Page) ([]domain.Segment, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id,code,name,description,created_at FROM segments WHERE id>$1 ORDER BY id LIMIT $2`, p.AfterID, p.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Segment{}
	for rows.Next() {
		v, err := scanSegment(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
func (s *Store) PatchSegment(ctx context.Context, id int64, p domain.PatchSegment) (domain.Segment, error) {
	return scanSegment(s.Pool.QueryRow(ctx, `UPDATE segments SET name=COALESCE($2,name),description=COALESCE($3,description) WHERE id=$1 RETURNING id,code,name,description,created_at`, id, p.Name, p.Description))
}
func (s *Store) DeleteSegment(ctx context.Context, id int64) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM segments WHERE id=$1`, id)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrNotFound
	}
	return nil
}
func (s *Store) CreateUser(ctx context.Context, id int64) (domain.User, error) {
	var u domain.User
	err := s.Pool.QueryRow(ctx, `INSERT INTO users(id) VALUES($1) RETURNING id,created_at`, id).Scan(&u.ID, &u.CreatedAt)
	return u, mapError(err)
}

func (s *Store) SetMembership(ctx context.Context, uid, sid int64, add bool) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var found int64
	if err = tx.QueryRow(ctx, `SELECT id FROM segments WHERE id=$1 FOR UPDATE`, sid).Scan(&found); err != nil {
		return mapError(err)
	}
	if err = tx.QueryRow(ctx, `SELECT id FROM users WHERE id=$1 FOR KEY SHARE`, uid).Scan(&found); err != nil {
		return mapError(err)
	}
	if add {
		_, err = tx.Exec(ctx, `INSERT INTO user_segments(user_id,segment_id) VALUES($1,$2) ON CONFLICT (user_id,segment_id) DO NOTHING`, uid, sid)
	} else {
		_, err = tx.Exec(ctx, `DELETE FROM user_segments WHERE user_id=$1 AND segment_id=$2`, uid, sid)
	}
	if err != nil {
		return mapError(err)
	}
	return mapError(tx.Commit(ctx))
}

func (s *Store) UserSegments(ctx context.Context, uid int64) ([]domain.Segment, error) {
	rows, err := s.Pool.Query(ctx, `SELECT s.id,s.code,s.name,s.description,s.created_at
 FROM users u LEFT JOIN user_segments us ON us.user_id=u.id
 LEFT JOIN segments s ON s.id=us.segment_id WHERE u.id=$1 ORDER BY us.segment_id`, uid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.Segment{}
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
			segment = domain.Segment{ID: *id, Code: *code, Name: *name, Description: *description, CreatedAt: *timestamp}
			out = append(out, segment)
		}
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if !exists {
		return nil, domain.ErrNotFound
	}
	return out, nil
}
