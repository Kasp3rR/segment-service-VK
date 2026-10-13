package service

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type Repository interface {
	CreateSegment(context.Context, domain.CreateSegment) (domain.Segment, error)
	GetSegment(context.Context, int64) (domain.Segment, error)
	ListSegments(context.Context, domain.Page) ([]domain.Segment, error)
	PatchSegment(context.Context, int64, domain.PatchSegment) (domain.Segment, error)
	DeleteSegment(context.Context, int64) error
	CreateUser(context.Context, int64) (domain.User, error)
	SetMembership(context.Context, int64, int64, bool) error
	UserSegments(context.Context, int64) ([]domain.Segment, error)
}
type Service struct{ repo Repository }

func New(repo Repository) *Service { return &Service{repo: repo} }

var codePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{0,63}$`)

func invalid(message string) error   { return fmt.Errorf("%w: %s", domain.ErrInvalid, message) }
func validName(name string) bool     { n := utf8.RuneCountInString(name); return n >= 1 && n <= 200 }
func validDescription(v string) bool { return utf8.RuneCountInString(v) <= 4000 }
func (s *Service) CreateSegment(ctx context.Context, in domain.CreateSegment) (domain.Segment, error) {
	in.Name = strings.TrimSpace(in.Name)
	if !codePattern.MatchString(in.Code) {
		return domain.Segment{}, invalid("code must match [A-Z][A-Z0-9_]{0,63}")
	}
	if !validName(in.Name) {
		return domain.Segment{}, invalid("name must contain 1 to 200 characters")
	}
	if !validDescription(in.Description) {
		return domain.Segment{}, invalid("description must contain at most 4000 characters")
	}
	return s.repo.CreateSegment(ctx, in)
}
func (s *Service) GetSegment(ctx context.Context, id int64) (domain.Segment, error) {
	if id <= 0 {
		return domain.Segment{}, invalid("id must be positive")
	}
	return s.repo.GetSegment(ctx, id)
}
func (s *Service) ListSegments(ctx context.Context, p domain.Page) ([]domain.Segment, error) {
	if p.Limit < 1 || p.Limit > 1000 || p.AfterID < 0 {
		return nil, invalid("limit must be 1..1000 and after_id nonnegative")
	}
	return s.repo.ListSegments(ctx, p)
}
func (s *Service) PatchSegment(ctx context.Context, id int64, p domain.PatchSegment) (domain.Segment, error) {
	if id <= 0 {
		return domain.Segment{}, invalid("id must be positive")
	}
	if p.Name == nil && p.Description == nil {
		return domain.Segment{}, invalid("provide name or description")
	}
	if p.Name != nil {
		name := strings.TrimSpace(*p.Name)
		if !validName(name) {
			return domain.Segment{}, invalid("name must contain 1 to 200 characters")
		}
		p.Name = &name
	}
	if p.Description != nil && !validDescription(*p.Description) {
		return domain.Segment{}, invalid("description must contain at most 4000 characters")
	}
	return s.repo.PatchSegment(ctx, id, p)
}
func (s *Service) DeleteSegment(ctx context.Context, id int64) error {
	if id <= 0 {
		return invalid("id must be positive")
	}
	return s.repo.DeleteSegment(ctx, id)
}
func (s *Service) CreateUser(ctx context.Context, id int64) (domain.User, error) {
	if id <= 0 {
		return domain.User{}, invalid("user_id must be positive")
	}
	return s.repo.CreateUser(ctx, id)
}
func (s *Service) SetMembership(ctx context.Context, uid, sid int64, add bool) error {
	if uid <= 0 || sid <= 0 {
		return invalid("user_id and segment_id must be positive")
	}
	return s.repo.SetMembership(ctx, uid, sid, add)
}
func (s *Service) UserSegments(ctx context.Context, id int64) ([]domain.Segment, error) {
	if id <= 0 {
		return nil, invalid("user_id must be positive")
	}
	return s.repo.UserSegments(ctx, id)
}
