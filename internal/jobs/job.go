package jobs

import "time"

type JobID string

type Job[T any] struct {
	ID        JobID
	Payload   T
	CreatedAt time.Time
}
