package service

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) CreateUser(ctx context.Context, id int64) (domain.User, error) {
	if id <= 0 {
		return domain.User{}, invalid("user_id must be positive")
	}
	return s.repo.CreateUser(ctx, id)
}
