package service

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) UserSegments(ctx context.Context, id int64) ([]domain.Segment, error) {
	if id <= 0 {
		return nil, invalid("user_id must be positive")
	}
	return s.repo.UserSegments(ctx, id)
}
