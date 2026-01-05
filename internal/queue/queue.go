package queue

import (
	"context"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type Queue[T any] interface {
	Enqueue(ctx context.Context, job T) error
	Dequeue(ctx context.Context) (T, error)
}

// PersistentQueue extends Queue with persistence operations
type PersistentQueue interface {
	Queue[*jobs.Envelope]
	CompleteJob(ctx context.Context, jobID jobs.JobID, attempts int) error
	FailJob(ctx context.Context, jobID jobs.JobID, errorMsg string) error
	RequeueJob(ctx context.Context, env *jobs.Envelope) error
	GetJob(ctx context.Context, jobID jobs.JobID) (*JobRecord, bool)
	ListJobsByStatus(ctx context.Context, status string) []*JobRecord
	MoveToDLQ(ctx context.Context, env *jobs.Envelope, errorMsg string) error
	ListDLQJobs(ctx context.Context) []*JobRecord
	RequeueDLQJob(ctx context.Context, jobID jobs.JobID) error
	CancelJob(ctx context.Context, jobID jobs.JobID) error
	Close() error
}
