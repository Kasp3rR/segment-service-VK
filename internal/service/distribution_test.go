package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

func TestDistributionValidation(t *testing.T) {
	repo := &fakeRepository{}
	s := NewService(repo)
	ctx := context.Background()
	for _, tc := range []struct {
		id int64
		p  string
	}{{0, "30"}, {1, "-1"}, {1, `"30"`}, {1, "101"}, {1, ""}} {
		if _, err := s.StartDistribution(ctx, tc.id, tc.p); !errors.Is(err, domain.ErrInvalid) {
			t.Fatal(err)
		}
	}
	if repo.calls != 0 {
		t.Fatal("invalid input reached repository")
	}
	if _, err := s.StartDistribution(ctx, 7, "30"); err != nil || repo.sid != 7 {
		t.Fatal(err)
	}
}
func TestDistributionJobLookup(t *testing.T) {
	repo := &fakeRepository{}
	s := NewService(repo)
	ctx := context.Background()
	if _, err := s.GetDistribution(ctx, "bad"); !errors.Is(err, domain.ErrInvalid) || repo.calls != 0 {
		t.Fatal(err)
	}
	id := "11111111-2222-4333-8444-555555555555"
	job, err := s.GetDistribution(ctx, id)
	if err != nil || job.ID != id {
		t.Fatal(err)
	}
}
func TestDistributionRepositoryErrors(t *testing.T) {
	for _, cause := range []error{domain.ErrConflict, domain.ErrNotFound, context.DeadlineExceeded} {
		s := NewService(&fakeRepository{err: cause})
		if _, err := s.StartDistribution(context.Background(), 1, "30"); !errors.Is(err, cause) {
			t.Fatal(err)
		}
		if _, err := s.GetDistribution(context.Background(), "11111111-2222-4333-8444-555555555555"); !errors.Is(err, cause) {
			t.Fatal(err)
		}
	}
}
