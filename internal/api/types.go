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
