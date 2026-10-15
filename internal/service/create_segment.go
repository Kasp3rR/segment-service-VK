package service

import (
	"context"
	"strings"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func (s *Service) CreateSegment(ctx context.Context, request domain.CreateSegment) (domain.Segment, error) {
	request.Name = strings.TrimSpace(request.Name)
	if !codePattern.MatchString(request.Code) {
		return domain.Segment{}, invalid("code must match [A-Z][A-Z0-9_]{0,63}")
	}
	if !validName(request.Name) {
		return domain.Segment{}, invalid("name must contain 1 to 200 characters")
	}
	if !validDescription(request.Description) {
		return domain.Segment{}, invalid("description must contain at most 4000 characters")
	}
	return s.repo.CreateSegment(ctx, request)
}
