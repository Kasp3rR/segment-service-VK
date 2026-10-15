package service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

type fakeRepository struct {
	calls    int
	err      error
	input    domain.CreateSegment
	patch    domain.PatchSegment
	uid, sid int64
	add      bool
	page     domain.Page
}

func (f *fakeRepository) CreateSegment(_ context.Context, in domain.CreateSegment) (domain.Segment, error) {
	f.calls++
	f.input = in
	return domain.Segment{ID: 1, Code: in.Code, Name: in.Name, Description: in.Description}, f.err
}
func (f *fakeRepository) GetSegment(_ context.Context, id int64) (domain.Segment, error) {
	f.calls++
	f.sid = id
	return domain.Segment{ID: id}, f.err
}
func (f *fakeRepository) ListSegments(_ context.Context, p domain.Page) ([]domain.Segment, error) {
	f.calls++
	f.page = p
	return []domain.Segment{}, f.err
}
func (f *fakeRepository) PatchSegment(_ context.Context, id int64, p domain.PatchSegment) (domain.Segment, error) {
	f.calls++
	f.sid = id
	f.patch = p
	return domain.Segment{ID: id}, f.err
}
func (f *fakeRepository) DeleteSegment(_ context.Context, id int64) error {
	f.calls++
	f.sid = id
	return f.err
}
func (f *fakeRepository) CreateUser(_ context.Context, id int64) (domain.User, error) {
	f.calls++
	f.uid = id
	return domain.User{ID: id}, f.err
}
func (f *fakeRepository) SetMembership(_ context.Context, uid, sid int64, add bool) error {
	f.calls++
	f.uid = uid
	f.sid = sid
	f.add = add
	return f.err
}
func (f *fakeRepository) UserSegments(_ context.Context, id int64) ([]domain.Segment, error) {
	f.calls++
	f.uid = id
	return []domain.Segment{}, f.err
}
func ptr(s string) *string { return &s }
func TestCreateSegmentValidation(t *testing.T) {
	for _, tc := range []struct {
		name, code, title, description string
		valid                          bool
	}{
		{"normal", "MAIL_GPT", "GPT", "Письма", true}, {"single_character_code", "A", "x", "", true},
		{"unicode_name", "A", "  Почта  ", "", true}, {"max_code", strings.Repeat("A", 64), "x", "", true},
		{"max_unicode_name", "A", strings.Repeat("я", 200), "", true}, {"max_description", "A", "x", strings.Repeat("я", 4000), true},
		{"empty_code", "", "x", "", false}, {"lowercase_code", "mail", "x", "", false}, {"numeric_start", "1A", "x", "", false},
		{"space_in_code", "MAIL GPT", "x", "", false}, {"long_code", strings.Repeat("A", 65), "x", "", false},
		{"empty_name", "A", "", "", false}, {"blank_name", "A", " \t\n", "", false}, {"long_name", "A", strings.Repeat("я", 201), "", false},
		{"long_description", "A", "x", strings.Repeat("я", 4001), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{}
			s := NewService(repo)
			got, err := s.CreateSegment(context.Background(), domain.CreateSegment{Code: tc.code, Name: tc.title, Description: tc.description})
			if tc.valid {
				if err != nil || repo.calls != 1 || got.Name != strings.TrimSpace(tc.title) {
					t.Fatalf("got=%+v calls=%d err=%v", got, repo.calls, err)
				}
			} else if !errors.Is(err, domain.ErrInvalid) || repo.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, repo.calls)
			}
		})
	}
}
func TestPatchValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		id    int64
		patch domain.PatchSegment
		valid bool
	}{
		{"name", 1, domain.PatchSegment{Name: ptr("  New  ")}, true}, {"clear_description", 1, domain.PatchSegment{Description: ptr("")}, true},
		{"both", 1, domain.PatchSegment{Name: ptr("New"), Description: ptr("x")}, true}, {"bad_id", 0, domain.PatchSegment{Name: ptr("x")}, false},
		{"empty_patch", 1, domain.PatchSegment{}, false}, {"blank_name", 1, domain.PatchSegment{Name: ptr(" ")}, false},
		{"long_name", 1, domain.PatchSegment{Name: ptr(strings.Repeat("a", 201))}, false}, {"long_description", 1, domain.PatchSegment{Description: ptr(strings.Repeat("я", 4001))}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{}
			_, err := NewService(repo).PatchSegment(context.Background(), tc.id, tc.patch)
			if tc.valid {
				if err != nil || repo.calls != 1 {
					t.Fatal(err)
				}
				if tc.patch.Name != nil && *repo.patch.Name != strings.TrimSpace(*tc.patch.Name) {
					t.Fatal("name not normalized")
				}
			} else if !errors.Is(err, domain.ErrInvalid) || repo.calls != 0 {
				t.Fatalf("err=%v calls=%d", err, repo.calls)
			}
		})
	}
}
func TestPageValidation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		p     domain.Page
		valid bool
	}{{"default", domain.Page{Limit: 100}, true}, {"max", domain.Page{Limit: 1000, AfterID: 100}, true}, {"zero", domain.Page{}, false}, {"negative", domain.Page{Limit: -1}, false}, {"too_large", domain.Page{Limit: 1001}, false}, {"bad_cursor", domain.Page{Limit: 1, AfterID: -1}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{}
			_, err := NewService(repo).ListSegments(context.Background(), tc.p)
			if tc.valid {
				if err != nil || repo.page != tc.p {
					t.Fatal(err)
				}
			} else if !errors.Is(err, domain.ErrInvalid) || repo.calls != 0 {
				t.Fatal("invalid page accepted")
			}
		})
	}
}
func TestOperationsValidateAndPropagate(t *testing.T) {
	ctx := context.Background()
	sentinel := errors.New("database unavailable")
	for _, tc := range []struct {
		name    string
		valid   func(*Service) error
		invalid func(*Service) error
		check   func(*fakeRepository) bool
	}{
		{"get_segment", func(s *Service) error { _, e := s.GetSegment(ctx, 7); return e }, func(s *Service) error { _, e := s.GetSegment(ctx, 0); return e }, func(r *fakeRepository) bool { return r.sid == 7 }},
		{"delete_segment", func(s *Service) error { return s.DeleteSegment(ctx, 7) }, func(s *Service) error { return s.DeleteSegment(ctx, -1) }, func(r *fakeRepository) bool { return r.sid == 7 }},
		{"create_user", func(s *Service) error { _, e := s.CreateUser(ctx, 7); return e }, func(s *Service) error { _, e := s.CreateUser(ctx, 0); return e }, func(r *fakeRepository) bool { return r.uid == 7 }},
		{"add_membership", func(s *Service) error { return s.SetMembership(ctx, 7, 8, true) }, func(s *Service) error { return s.SetMembership(ctx, 0, 8, true) }, func(r *fakeRepository) bool { return r.uid == 7 && r.sid == 8 && r.add }},
		{"remove_membership", func(s *Service) error { return s.SetMembership(ctx, 7, 8, false) }, func(s *Service) error { return s.SetMembership(ctx, 7, 0, false) }, func(r *fakeRepository) bool { return r.uid == 7 && r.sid == 8 && !r.add }},
		{"user_segments", func(s *Service) error { _, e := s.UserSegments(ctx, 7); return e }, func(s *Service) error { _, e := s.UserSegments(ctx, -1); return e }, func(r *fakeRepository) bool { return r.uid == 7 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &fakeRepository{err: sentinel}
			s := NewService(repo)
			if err := tc.invalid(s); !errors.Is(err, domain.ErrInvalid) || repo.calls != 0 {
				t.Fatal("invalid request reached repository")
			}
			if err := tc.valid(s); !errors.Is(err, sentinel) || repo.calls != 1 || !tc.check(repo) {
				t.Fatalf("err=%v repo=%+v", err, repo)
			}
		})
	}
}
func TestWriteErrorsPropagate(t *testing.T) {
	for _, e := range []error{domain.ErrConflict, domain.ErrNotFound, context.Canceled} {
		r := &fakeRepository{err: e}
		s := NewService(r)
		if _, got := s.CreateSegment(context.Background(), domain.CreateSegment{Code: "A", Name: "x"}); !errors.Is(got, e) {
			t.Fatal(got)
		}
		if _, got := s.PatchSegment(context.Background(), 1, domain.PatchSegment{Name: ptr("x")}); !errors.Is(got, e) {
			t.Fatal(got)
		}
		if _, got := s.ListSegments(context.Background(), domain.Page{Limit: 1}); !errors.Is(got, e) {
			t.Fatal(got)
		}
	}
}

func (f *fakeRepository) StartDistribution(_ context.Context, id int64, percent string) (domain.Job, error) {
	f.calls++
	f.sid = id
	return domain.Job{SegmentID: id}, f.err
}
func (f *fakeRepository) GetDistribution(_ context.Context, id string) (domain.Job, error) {
	f.calls++
	return domain.Job{ID: id}, f.err
}
