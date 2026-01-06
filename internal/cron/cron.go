package cron

import (
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type CronJobID string

type CronJob struct {
	ID          CronJobID
	Name        string
	CronExpr    string
	JobType     jobs.JobType
	Payload     []byte
	Priority    int
	MaxRetries  int
	Timeout     time.Duration
	Enabled     bool
	NextRun     *time.Time
	LastRun     *time.Time
	CreatedAt   time.Time
	UpdatedAt   time.Time
}
