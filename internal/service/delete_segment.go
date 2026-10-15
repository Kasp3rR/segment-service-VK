package service

import (
	"context"
)

func (s *Service) DeleteSegment(ctx context.Context, id int64) error {
	if id <= 0 {
		return invalid("id must be positive")
	}
	return s.repo.DeleteSegment(ctx, id)
}
