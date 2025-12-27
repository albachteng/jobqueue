package api

import (
	"encoding/json"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type EnqueueRequest struct {
	Type    jobs.JobType    `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type EnqueueResponse struct {
	JobID  jobs.JobID `json:"job_id"`
	Status string     `json:"status"`
}
