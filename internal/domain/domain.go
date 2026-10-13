package domain

import (
	"errors"
	"time"
)

var (
	ErrInvalid  = errors.New("invalid input")
	ErrNotFound = errors.New("resource not found")
	ErrConflict = errors.New("resource already exists")
)

type Segment struct {
	ID          int64     `json:"id"`
	Code        string    `json:"code"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"created_at"`
}
type User struct {
	ID        int64     `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}
type CreateSegment struct{ Code, Name, Description string }
type PatchSegment struct{ Name, Description *string }
type Page struct {
	Limit   int
	AfterID int64
}
