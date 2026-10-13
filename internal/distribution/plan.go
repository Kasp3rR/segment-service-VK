package distribution

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/Kasp3rR/segment-service-VK/internal/domain"
)

var numberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
var jobPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func ValidJobID(id string) bool { return jobPattern.MatchString(id) }
func Target(total int64, percent string) (int64, error) {
	if total < 0 || len(percent) > 64 || !numberPattern.MatchString(percent) {
		return 0, fmt.Errorf("%w: percent must be a JSON number between 0 and 100", domain.ErrInvalid)
	}
	if pos := strings.IndexAny(percent, "eE"); pos >= 0 {
		n, err := strconv.ParseInt(percent[pos+1:], 10, 32)
		if err != nil || n < -1000 || n > 1000 {
			return 0, fmt.Errorf("%w: percent exponent is too large", domain.ErrInvalid)
		}
	}
	ratio, ok := new(big.Rat).SetString(percent)
	if !ok || ratio.Sign() < 0 || ratio.Cmp(big.NewRat(100, 1)) > 0 {
		return 0, fmt.Errorf("%w: percent must be between 0 and 100", domain.ErrInvalid)
	}
	numerator := new(big.Int).Mul(ratio.Num(), big.NewInt(total))
	denominator := new(big.Int).Mul(ratio.Denom(), big.NewInt(100))
	return new(big.Int).Quo(numerator, denominator).Int64(), nil
}
func Score(segmentID, userID int64) [32]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(segmentID))
	binary.BigEndian.PutUint64(b[8:], uint64(userID))
	return sha256.Sum256(b[:])
}
func BuildPlan(segmentID int64, percent string, users []domain.SnapshotUser) (domain.Plan, error) {
	var plan domain.Plan
	if segmentID <= 0 {
		return plan, domain.ErrInvalid
	}
	target, err := Target(int64(len(users)), percent)
	if err != nil {
		return plan, err
	}
	plan.Target = target
	type candidate struct {
		id    int64
		score [32]byte
	}
	candidates := make([]candidate, 0, len(users))
	seen := make(map[int64]struct{}, len(users))
	for _, u := range users {
		if u.ID <= 0 {
			return plan, domain.ErrInvalid
		}
		if _, ok := seen[u.ID]; ok {
			return plan, domain.ErrInvalid
		}
		seen[u.ID] = struct{}{}
		if u.Assigned {
			plan.Existing++
		} else {
			candidates = append(candidates, candidate{u.ID, Score(segmentID, u.ID)})
		}
	}
	if plan.Existing > target {
		return plan, fmt.Errorf("%w: existing assignments (%d) exceed target (%d)", domain.ErrConflict, plan.Existing, target)
	}
	sort.Slice(candidates, func(i, j int) bool {
		cmp := bytes.Compare(candidates[i].score[:], candidates[j].score[:])
		if cmp == 0 {
			return candidates[i].id < candidates[j].id
		}
		return cmp < 0
	})
	plan.UserIDs = make([]int64, target-plan.Existing)
	for i := range plan.UserIDs {
		plan.UserIDs[i] = candidates[i].id
	}
	return plan, nil
}
func Batches(jobID string, segmentID int64, ids []int64) []domain.Batch {
	out := make([]domain.Batch, 0, (len(ids)+domain.BatchSize-1)/domain.BatchSize)
	for start := 0; start < len(ids); start += domain.BatchSize {
		end := min(start+domain.BatchSize, len(ids))
		out = append(out, domain.Batch{Version: domain.MessageVersion, JobID: jobID, SegmentID: segmentID, Number: len(out), UserIDs: append([]int64(nil), ids[start:end]...)})
	}
	return out
}
func ValidateBatch(b domain.Batch) error {
	if b.Version != domain.MessageVersion || !ValidJobID(b.JobID) || b.SegmentID <= 0 || b.Number < 0 || len(b.UserIDs) == 0 || len(b.UserIDs) > domain.BatchSize {
		return domain.ErrInvalidMessage
	}
	seen := make(map[int64]struct{}, len(b.UserIDs))
	for _, id := range b.UserIDs {
		if id <= 0 {
			return domain.ErrInvalidMessage
		}
		if _, ok := seen[id]; ok {
			return domain.ErrInvalidMessage
		}
		seen[id] = struct{}{}
	}
	return nil
}
