package api

import (
	"encoding/json"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type EnqueueRequest struct {
	Type        jobs.JobType    `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Priority    int             `json:"priority,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
}

type EnqueueResponse struct {
	JobID  jobs.JobID `json:"job_id"`
	Status string     `json:"status"`
}

type CreateCronJobRequest struct {
	Name       string          `json:"name"`
	CronExpr   string          `json:"cron_expr"`
	JobType    jobs.JobType    `json:"job_type"`
	Payload    json.RawMessage `json:"payload"`
	Priority   int             `json:"priority,omitempty"`
	MaxRetries int             `json:"max_retries,omitempty"`
	Timeout    string          `json:"timeout,omitempty"`
	Enabled    bool            `json:"enabled"`
}

type CreateCronJobResponse struct {
	CronJobID string `json:"cron_job_id"`
	Status    string `json:"status"`
}

type UpdateCronJobRequest struct {
	Name       string          `json:"name,omitempty"`
	CronExpr   string          `json:"cron_expr,omitempty"`
	JobType    jobs.JobType    `json:"job_type,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
	Priority   int             `json:"priority,omitempty"`
	MaxRetries int             `json:"max_retries,omitempty"`
	Timeout    string          `json:"timeout,omitempty"`
	Enabled    bool            `json:"enabled"`
}
