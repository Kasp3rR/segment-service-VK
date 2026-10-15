package queue

import (
	"context"
	"time"
)

func Backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	return min(time.Duration(1<<min(attempt, 6))*100*time.Millisecond, 5*time.Second)
}

func Pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
