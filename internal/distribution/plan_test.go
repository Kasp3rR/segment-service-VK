package distribution

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

const validJob = "11111111-2222-4333-8444-555555555555"

func TestTargetExactArithmetic(t *testing.T) {
	for _, tc := range []struct {
		name    string
		total   int64
		percent string
		want    int64
	}{{"empty", 0, "30", 0}, {"floor", 7, "30", 2}, {"zero", 99, "0", 0}, {"all", 99, "100", 99}, {"decimal", 100, "29", 29}, {"fraction", 10000, "0.29", 29}, {"exponent", 1000, "3e1", 300}, {"tiny", 1000, "1e-1000", 0}, {"large_population", math.MaxInt64, "100", math.MaxInt64}, {"negative_zero", 10, "-0", 0}} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Target(tc.total, tc.percent)
			if err != nil || got != tc.want {
				t.Fatalf("target=%d want=%d err=%v", got, tc.want, err)
			}
		})
	}
}
func TestTargetRejectsInvalid(t *testing.T) {
	for _, s := range []string{"", "-1", "100.0001", "NaN", "Inf", "null", `"30"`, "1/2", "01", ".5", "1e1001", "1e-1001", "1e999999999999999999", "1e1000", strings.Repeat("1", 65)} {
		t.Run(s, func(t *testing.T) {
			if _, err := Target(100, s); !errors.Is(err, domain.ErrInvalid) {
				t.Fatal(err)
			}
		})
	}
	if _, err := Target(-1, "20"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
}
func TestPlanReproducibleAndPreservesExisting(t *testing.T) {
	users := []domain.SnapshotUser{}
	for i := int64(1); i <= 100; i++ {
		users = append(users, domain.SnapshotUser{ID: i, Assigned: i <= 5})
	}
	a, err := BuildPlan(7, "30", users)
	if err != nil || a.Target != 30 || a.Existing != 5 || len(a.UserIDs) != 25 {
		t.Fatalf("%+v %v", a, err)
	}
	for left, right := 0, len(users)-1; left < right; left, right = left+1, right-1 {
		users[left], users[right] = users[right], users[left]
	}
	b, err := BuildPlan(7, "30", users)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("selection depends on row order")
	}
	for _, id := range a.UserIDs {
		if id <= 5 {
			t.Fatal("existing user selected twice")
		}
	}
	c, err := BuildPlan(8, "30", users)
	if err != nil || reflect.DeepEqual(a.UserIDs, c.UserIDs) {
		t.Fatal("different segments must have different rankings")
	}
}
func TestPlanConflicts(t *testing.T) {
	_, err := BuildPlan(1, "0", []domain.SnapshotUser{{ID: 1, Assigned: true}})
	if !errors.Is(err, domain.ErrConflict) {
		t.Fatal(err)
	}
}
func TestPlanInvalidSnapshot(t *testing.T) {
	for _, tc := range []struct {
		id    int64
		p     string
		users []domain.SnapshotUser
	}{{0, "10", nil}, {1, "bad", nil}, {1, "100", []domain.SnapshotUser{{ID: 0}}}, {1, "100", []domain.SnapshotUser{{ID: 1}, {ID: 1}}}} {
		if _, err := BuildPlan(tc.id, tc.p, tc.users); !errors.Is(err, domain.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
func TestBatchBoundaries(t *testing.T) {
	for _, n := range []int{0, 1, 4999, 5000, 5001, 10000, 10001} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			ids := make([]int64, n)
			for i := range ids {
				ids[i] = int64(i + 1)
			}
			b := Batches(validJob, 1, ids)
			if len(b) != (n+4999)/5000 {
				t.Fatal(len(b))
			}
			var combined []int64
			for i, v := range b {
				if v.Number != i || ValidateBatch(v) != nil {
					t.Fatalf("invalid batch %d", i)
				}
				combined = append(combined, v.UserIDs...)
			}
			if len(combined) != n {
				t.Fatal("lost users")
			}
			for i, id := range combined {
				if id != int64(i+1) {
					t.Fatal("wrong order")
				}
			}
			if n > 0 {
				ids[0] = 999
				if b[0].UserIDs[0] != 1 {
					t.Fatal("batch aliases input")
				}
			}
		})
	}
}
func TestValidateBatch(t *testing.T) {
	base := domain.Batch{Version: 1, JobID: validJob, SegmentID: 1, Number: 0, UserIDs: []int64{1, 2}}
	for _, mutate := range []func(*domain.Batch){func(b *domain.Batch) { b.Version = 2 }, func(b *domain.Batch) { b.JobID = "bad" }, func(b *domain.Batch) { b.SegmentID = 0 }, func(b *domain.Batch) { b.Number = -1 }, func(b *domain.Batch) { b.UserIDs = nil }, func(b *domain.Batch) { b.UserIDs = make([]int64, 5001) }, func(b *domain.Batch) { b.UserIDs = []int64{0} }, func(b *domain.Batch) { b.UserIDs = []int64{1, 1} }} {
		b := base
		mutate(&b)
		if !errors.Is(ValidateBatch(b), domain.ErrInvalidMessage) {
			t.Fatal("accepted invalid batch")
		}
	}
	if err := ValidateBatch(base); err != nil {
		t.Fatal(err)
	}
}
