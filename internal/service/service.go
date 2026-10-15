package service

import (
	"context"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type Repository interface {
	StartDistribution(context.Context, int64, string) (domain.Job, error)
	GetDistribution(context.Context, string) (domain.Job, error)
	CreateSegment(context.Context, domain.CreateSegment) (domain.Segment, error)
	GetSegment(context.Context, int64) (domain.Segment, error)
	ListSegments(context.Context, domain.Page) ([]domain.Segment, error)
	PatchSegment(context.Context, int64, domain.PatchSegment) (domain.Segment, error)
	DeleteSegment(context.Context, int64) error
	CreateUser(context.Context, int64) (domain.User, error)
	SetMembership(context.Context, int64, int64, bool) error
	UserSegments(context.Context, int64) ([]domain.Segment, error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{
		repo: repo,
	}
}
