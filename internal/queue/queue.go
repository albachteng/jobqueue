package queue

import (
	"context"
	"time"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
)

type Queue[T any] interface {
	Enqueue(ctx context.Context, job T) error
	Dequeue(ctx context.Context) (T, error)
}

// PersistentQueue extends Queue with persistence operations
type PersistentQueue interface {
	Queue[*jobs.Envelope]
	CompleteJob(ctx context.Context, jobID jobs.JobID) error
	FailJob(ctx context.Context, jobID jobs.JobID, errorMsg string) error
	RequeueJob(ctx context.Context, env *jobs.Envelope) error
	GetJob(ctx context.Context, jobID jobs.JobID) (*JobRecord, bool)
	ListJobsByStatus(ctx context.Context, status string) []*JobRecord
	MoveToDLQ(ctx context.Context, env *jobs.Envelope, errorMsg string) error
	ListDLQJobs(ctx context.Context) []*JobRecord
	RequeueDLQJob(ctx context.Context, jobID jobs.JobID) error
	CancelJob(ctx context.Context, jobID jobs.JobID) error
	CreateCronJob(ctx context.Context, cronJob *cron.CronJob) error
	GetCronJob(ctx context.Context, id cron.CronJobID) (*cron.CronJob, bool)
	ListCronJobs(ctx context.Context) []*cron.CronJob
	ListEnabledCronJobs(ctx context.Context) []*cron.CronJob
	UpdateCronJob(ctx context.Context, cronJob *cron.CronJob) error
	DeleteCronJob(ctx context.Context, id cron.CronJobID) error
	UpdateCronJobNextRun(ctx context.Context, id cron.CronJobID, nextRun time.Time) error
	UpdateCronJobLastRun(ctx context.Context, id cron.CronJobID, lastRun time.Time) error
	Close() error
}
