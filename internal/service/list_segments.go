package service

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) ListSegments(ctx context.Context, request domain.Page) ([]domain.Segment, error) {
	if request.Limit < 1 || request.Limit > 1000 || request.AfterID < 0 {
		return nil, invalid("limit must be 1..1000 and after_id nonnegative")
	}
	return s.repo.ListSegments(ctx, request)
}
