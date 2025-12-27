package tracking

import (
	"sync"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type JobInfo struct {
	Envelope   *jobs.Envelope
	Status     JobStatus
	Error      string
	StartedAt  *time.Time
	FinishedAt *time.Time
}

type JobTracker struct {
	mu   sync.RWMutex
	jobs map[jobs.JobID]*JobInfo
}

func NewJobTracker() *JobTracker {
	return &JobTracker{
		jobs: make(map[jobs.JobID]*JobInfo),
	}
}

func (t *JobTracker) Register(envelope *jobs.Envelope) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.jobs[envelope.ID] = &JobInfo{
		Envelope: envelope,
		Status:   StatusPending,
	}
}

func (t *JobTracker) MarkProcessing(id jobs.JobID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, exists := t.jobs[id]; exists {
		now := time.Now()
		info.Status = StatusProcessing
		info.StartedAt = &now
	}
}

func (t *JobTracker) MarkCompleted(id jobs.JobID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, exists := t.jobs[id]; exists {
		now := time.Now()
		info.Status = StatusCompleted
		info.FinishedAt = &now
	}
}

func (t *JobTracker) MarkFailed(id jobs.JobID, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, exists := t.jobs[id]; exists {
		now := time.Now()
		info.Status = StatusFailed
		info.Error = err.Error()
		info.FinishedAt = &now
	}
}

func (t *JobTracker) Get(id jobs.JobID) (*JobInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, exists := t.jobs[id]
	return info, exists
}

func (t *JobTracker) List() []*JobInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	jobs := make([]*JobInfo, 0, len(t.jobs))
	for _, info := range t.jobs {
		jobs = append(jobs, info)
	}
	return jobs
}

func (t *JobTracker) ListByStatus(status JobStatus) []*JobInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()

	jobs := make([]*JobInfo, 0)
	for _, info := range t.jobs {
		if info.Status == status {
			jobs = append(jobs, info)
		}
	}
	return jobs
}
