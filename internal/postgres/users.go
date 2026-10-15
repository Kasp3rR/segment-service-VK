package postgres

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Store) CreateUser(ctx context.Context, id int64) (domain.User, error) {
	const query = `INSERT INTO users(id) VALUES($1) RETURNING id,created_at`

	var user domain.User
	err := s.Pool.QueryRow(ctx, query, id).Scan(&user.ID, &user.CreatedAt)
	return user, mapError(err)
}
