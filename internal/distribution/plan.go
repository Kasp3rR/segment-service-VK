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

func ValidJobID(id string) bool {
	return jobPattern.MatchString(id)
}

func Target(total int64, percent string) (int64, error) {
	if total < 0 || len(percent) > 64 || !numberPattern.MatchString(percent) {
		return 0, fmt.Errorf("%w: percent must be a JSON number between 0 and 100", domain.ErrInvalid)
	}
	if pos := strings.IndexAny(percent, "eE"); pos >= 0 {
		exponent, err := strconv.ParseInt(percent[pos+1:], 10, 32)
		if err != nil || exponent < -1000 || exponent > 1000 {
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
	var data [16]byte
	binary.BigEndian.PutUint64(data[:8], uint64(segmentID))
	binary.BigEndian.PutUint64(data[8:], uint64(userID))
	return sha256.Sum256(data[:])
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
	seen := make(map[int64]bool, len(users))
	for _, user := range users {
		if user.ID <= 0 {
			return plan, domain.ErrInvalid
		}
		if seen[user.ID] {
			return plan, domain.ErrInvalid
		}
		seen[user.ID] = true
		if user.Assigned {
			plan.Existing++
		} else {
			candidates = append(candidates, candidate{
				id:    user.ID,
				score: Score(segmentID, user.ID),
			})
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

func Batches(jobID string, segmentID int64, userIDs []int64) []domain.Batch {
	batches := make([]domain.Batch, 0, (len(userIDs)+domain.BatchSize-1)/domain.BatchSize)
	for start := 0; start < len(userIDs); start += domain.BatchSize {
		end := start + domain.BatchSize
		if end > len(userIDs) {
			end = len(userIDs)
		}
		batchUsers := make([]int64, end-start)
		copy(batchUsers, userIDs[start:end])
		batches = append(batches, domain.Batch{
			Version:   domain.MessageVersion,
			JobID:     jobID,
			SegmentID: segmentID,
			Number:    len(batches),
			UserIDs:   batchUsers,
		})
	}
	return batches
}

func ValidateBatch(batch domain.Batch) error {
	if batch.Version != domain.MessageVersion || !ValidJobID(batch.JobID) || batch.SegmentID <= 0 || batch.Number < 0 || len(batch.UserIDs) == 0 || len(batch.UserIDs) > domain.BatchSize {
		return domain.ErrInvalidMessage
	}
	seen := make(map[int64]bool, len(batch.UserIDs))
	for _, id := range batch.UserIDs {
		if id <= 0 {
			return domain.ErrInvalidMessage
		}
		if seen[id] {
			return domain.ErrInvalidMessage
		}
		seen[id] = true
	}
	return nil
}
