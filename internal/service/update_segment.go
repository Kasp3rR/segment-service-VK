package service

import (
	"context"
	"strings"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) PatchSegment(ctx context.Context, id int64, request domain.PatchSegment) (domain.Segment, error) {
	if id <= 0 {
		return domain.Segment{}, invalid("id must be positive")
	}
	if request.Name == nil && request.Description == nil {
		return domain.Segment{}, invalid("provide name or description")
	}
	if request.Name != nil {
		name := strings.TrimSpace(*request.Name)
		if !validName(name) {
			return domain.Segment{}, invalid("name must contain 1 to 200 characters")
		}
		request.Name = &name
	}
	if request.Description != nil && !validDescription(*request.Description) {
		return domain.Segment{}, invalid("description must contain at most 4000 characters")
	}
	return s.repo.PatchSegment(ctx, id, request)
}
