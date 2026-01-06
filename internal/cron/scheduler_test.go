package cron

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

type mockQueue struct {
	enqueuedJobs []*jobs.Envelope
	cronJobs     []*CronJob
}

func (m *mockQueue) Enqueue(ctx context.Context, env *jobs.Envelope) error {
	m.enqueuedJobs = append(m.enqueuedJobs, env)
	return nil
}

func (m *mockQueue) ListEnabledCronJobs(ctx context.Context) []*CronJob {
	return m.cronJobs
}

func (m *mockQueue) UpdateCronJobNextRun(ctx context.Context, id CronJobID, nextRun time.Time) error {
	for _, job := range m.cronJobs {
		if job.ID == id {
			job.NextRun = &nextRun
			return nil
		}
	}
	return nil
}

func (m *mockQueue) UpdateCronJobLastRun(ctx context.Context, id CronJobID, lastRun time.Time) error {
	for _, job := range m.cronJobs {
		if job.ID == id {
			job.LastRun = &lastRun
			return nil
		}
	}
	return nil
}

func TestScheduler(t *testing.T) {
	t.Run("parse valid cron expression", func(t *testing.T) {
		scheduler := NewScheduler(nil, nil)

		nextRun, err := scheduler.CalculateNextRun("*/5 * * * *", time.Now())
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if nextRun.IsZero() {
			t.Error("expected next run to be calculated")
		}
	})

	t.Run("reject invalid cron expression", func(t *testing.T) {
		scheduler := NewScheduler(nil, nil)

		_, err := scheduler.CalculateNextRun("invalid", time.Now())
		if err == nil {
			t.Error("expected error for invalid cron expression")
		}
	})

	t.Run("process due cron jobs", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		now := time.Now()
		pastTime := now.Add(-1 * time.Minute)

		mock := &mockQueue{
			cronJobs: []*CronJob{
				{
					ID:         "cron-due",
					Name:       "Due Job",
					CronExpr:   "*/5 * * * *",
					JobType:    "echo",
					Payload:    payload,
					Priority:   5,
					MaxRetries: 3,
					Timeout:    30 * time.Second,
					Enabled:    true,
					NextRun:    &pastTime,
				},
			},
		}

		scheduler := NewScheduler(mock, nil)
		ctx := context.Background()

		err := scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(mock.enqueuedJobs) != 1 {
			t.Fatalf("expected 1 enqueued job, got %d", len(mock.enqueuedJobs))
		}

		env := mock.enqueuedJobs[0]
		if env.Type != "echo" {
			t.Errorf("expected job type 'echo', got %q", env.Type)
		}
		if env.Priority != 5 {
			t.Errorf("expected priority 5, got %d", env.Priority)
		}
		if env.MaxRetries != 3 {
			t.Errorf("expected max retries 3, got %d", env.MaxRetries)
		}
	})

	t.Run("skip jobs not yet due", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		futureTime := time.Now().Add(10 * time.Minute)

		mock := &mockQueue{
			cronJobs: []*CronJob{
				{
					ID:       "cron-future",
					Name:     "Future Job",
					CronExpr: "*/5 * * * *",
					JobType:  "echo",
					Payload:  payload,
					Enabled:  true,
					NextRun:  &futureTime,
				},
			},
		}

		scheduler := NewScheduler(mock, nil)
		ctx := context.Background()

		err := scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(mock.enqueuedJobs) != 0 {
			t.Errorf("expected 0 enqueued jobs, got %d", len(mock.enqueuedJobs))
		}
	})

	t.Run("calculate next run for unscheduled jobs", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)

		mock := &mockQueue{
			cronJobs: []*CronJob{
				{
					ID:       "cron-new",
					Name:     "New Job",
					CronExpr: "*/5 * * * *",
					JobType:  "echo",
					Payload:  payload,
					Enabled:  true,
					NextRun:  nil,
				},
			},
		}

		scheduler := NewScheduler(mock, nil)
		ctx := context.Background()

		err := scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if mock.cronJobs[0].NextRun == nil {
			t.Error("expected next run to be calculated")
		}
	})

	t.Run("update next run after processing", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		now := time.Now()
		pastTime := now.Add(-1 * time.Minute)

		mock := &mockQueue{
			cronJobs: []*CronJob{
				{
					ID:       "cron-update",
					Name:     "Update Job",
					CronExpr: "*/5 * * * *",
					JobType:  "echo",
					Payload:  payload,
					Enabled:  true,
					NextRun:  &pastTime,
				},
			},
		}

		scheduler := NewScheduler(mock, nil)
		ctx := context.Background()

		err := scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		cronJob := mock.cronJobs[0]
		if cronJob.NextRun == nil {
			t.Fatal("expected next run to be updated")
		}

		if cronJob.NextRun.Before(now) {
			t.Error("expected next run to be in the future")
		}

		if cronJob.LastRun == nil {
			t.Error("expected last run to be set")
		}
	})

	t.Run("handle multiple due jobs", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		pastTime := time.Now().Add(-1 * time.Minute)

		mock := &mockQueue{
			cronJobs: []*CronJob{
				{
					ID:       "cron-1",
					Name:     "Job 1",
					CronExpr: "*/5 * * * *",
					JobType:  "echo",
					Payload:  payload,
					Enabled:  true,
					NextRun:  &pastTime,
				},
				{
					ID:       "cron-2",
					Name:     "Job 2",
					CronExpr: "*/10 * * * *",
					JobType:  "echo",
					Payload:  payload,
					Enabled:  true,
					NextRun:  &pastTime,
				},
			},
		}

		scheduler := NewScheduler(mock, nil)
		ctx := context.Background()

		err := scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if len(mock.enqueuedJobs) != 2 {
			t.Errorf("expected 2 enqueued jobs, got %d", len(mock.enqueuedJobs))
		}
	})
}

func TestSchedulerIntegration(t *testing.T) {
	dbPath := t.TempDir() + "/scheduler_integration_test.db"
	q, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	ctx := context.Background()
	payload := json.RawMessage(`{"message": "integration test"}`)

	pastTime := time.Now().Add(-1 * time.Minute)
	cronJob := &CronJob{
		ID:         "integration-cron",
		Name:       "Integration Test Cron",
		CronExpr:   "*/5 * * * *",
		JobType:    "echo",
		Payload:    payload,
		Priority:   10,
		MaxRetries: 5,
		Timeout:    60 * time.Second,
		Enabled:    true,
		NextRun:    &pastTime,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	err = q.CreateCronJob(ctx, cronJob)
	if err != nil {
		t.Fatalf("failed to create cron job: %v", err)
	}

	scheduler := NewScheduler(q, nil)

	err = scheduler.ProcessDueJobs(ctx)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	updatedCronJob, exists := q.GetCronJob(ctx, "integration-cron")
	if !exists {
		t.Fatal("expected cron job to exist")
	}

	if updatedCronJob.NextRun == nil {
		t.Error("expected next run to be set")
	}

	if updatedCronJob.LastRun == nil {
		t.Error("expected last run to be set")
	}

	jobs := q.ListJobsByStatus(ctx, "pending")
	if len(jobs) != 1 {
		t.Fatalf("expected 1 pending job, got %d", len(jobs))
	}

	job := jobs[0]
	if job.Type != "echo" {
		t.Errorf("expected job type 'echo', got %q", job.Type)
	}
	if job.Priority != 10 {
		t.Errorf("expected priority 10, got %d", job.Priority)
	}
	if job.MaxRetries != 5 {
		t.Errorf("expected max retries 5, got %d", job.MaxRetries)
	}
}
