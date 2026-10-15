package service

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) GetSegment(ctx context.Context, id int64) (domain.Segment, error) {
	if id <= 0 {
		return domain.Segment{}, invalid("id must be positive")
	}
	return s.repo.GetSegment(ctx, id)
}
