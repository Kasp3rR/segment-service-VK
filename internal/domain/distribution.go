package domain

import (
	"encoding/json"
	"errors"
	"time"
)

const BatchSize = 5000
const Consumers = 4
const MessageVersion = 1

var ErrInvalidMessage = errors.New("invalid distribution message")
var ErrBatchFailed = errors.New("permanent distribution failure")

type Job struct {
	ID               string      `json:"id"`
	SegmentID        int64       `json:"segment_id"`
	SegmentCode      string      `json:"segment_code"`
	Percent          json.Number `json:"percent"`
	SnapshotUsers    int64       `json:"snapshot_users"`
	TargetUsers      int64       `json:"target_users"`
	ExistingUsers    int64       `json:"existing_users"`
	PlannedUsers     int64       `json:"planned_users"`
	AssignedUsers    int64       `json:"assigned_users"`
	BatchCount       int         `json:"batch_count"`
	ProcessedBatches int         `json:"processed_batches"`
	Status           string      `json:"status"`
	LastError        string      `json:"last_error"`
	RetryCount       int         `json:"retry_count"`
	CreatedAt        time.Time   `json:"created_at"`
	CompletedAt      *time.Time  `json:"completed_at"`
}

type SnapshotUser struct {
	ID       int64
	Assigned bool
}
type Plan struct {
	Target, Existing int64
	UserIDs          []int64
}
type Batch struct {
	Version   int     `json:"version"`
	JobID     string  `json:"job_id"`
	SegmentID int64   `json:"segment_id"`
	Number    int     `json:"batch_number"`
	UserIDs   []int64 `json:"user_ids"`
}
type Delivery struct {
	Topic      string
	Partition  int
	Offset     int64
	ConsumerID string
}
