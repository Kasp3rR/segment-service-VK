package service

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/distribution"
	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) StartDistribution(ctx context.Context, segmentID int64, percent string) (domain.Job, error) {
	if segmentID <= 0 {
		return domain.Job{}, invalid("segment_id must be positive")
	}
	if _, err := distribution.Target(0, percent); err != nil {
		return domain.Job{}, err
	}
	return s.repo.StartDistribution(ctx, segmentID, percent)
}

func (s *Service) GetDistribution(ctx context.Context, id string) (domain.Job, error) {
	if !distribution.ValidJobID(id) {
		return domain.Job{}, invalid("job_id must be a UUID")
	}
	return s.repo.GetDistribution(ctx, id)
}
