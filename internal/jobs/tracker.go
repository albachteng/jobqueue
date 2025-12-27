package jobs

import (
	"sync"
	"time"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type JobInfo struct {
	Envelope   *Envelope
	Status     JobStatus
	Error      string
	StartedAt  *time.Time
	FinishedAt *time.Time
}

type JobTracker struct {
	mu   sync.RWMutex
	jobs map[JobID]*JobInfo
}

func NewJobTracker() *JobTracker {
	return &JobTracker{
		jobs: make(map[JobID]*JobInfo),
	}
}

func (t *JobTracker) Register(envelope *Envelope) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.jobs[envelope.ID] = &JobInfo{
		Envelope: envelope,
		Status:   StatusPending,
	}
}

func (t *JobTracker) MarkProcessing(id JobID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, exists := t.jobs[id]; exists {
		now := time.Now()
		info.Status = StatusProcessing
		info.StartedAt = &now
	}
}

func (t *JobTracker) MarkCompleted(id JobID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, exists := t.jobs[id]; exists {
		now := time.Now()
		info.Status = StatusCompleted
		info.FinishedAt = &now
	}
}

func (t *JobTracker) MarkFailed(id JobID, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if info, exists := t.jobs[id]; exists {
		now := time.Now()
		info.Status = StatusFailed
		info.Error = err.Error()
		info.FinishedAt = &now
	}
}

func (t *JobTracker) Get(id JobID) (*JobInfo, bool) {
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
