package service

import (
	"context"
)

func (s *Service) SetMembership(ctx context.Context, userID, segmentID int64, add bool) error {
	if userID <= 0 || segmentID <= 0 {
		return invalid("user_id and segment_id must be positive")
	}
	return s.repo.SetMembership(ctx, userID, segmentID, add)
}
