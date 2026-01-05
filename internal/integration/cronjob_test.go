package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/worker"
	"log/slog"
	"os"
)

func TestCronJob_EndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/cronjob_e2e_test.db"
	persQueue, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	ctx := context.Background()
	registry := jobs.NewRegistry()

	executionCount := 0
	registry.MustRegister("cron-echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		executionCount++
		t.Logf("Cron job executed, count: %d", executionCount)
		return nil
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
	dispatcherCtx, dispatcherCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dispatcherCancel()

	if err := dispatcher.Start(dispatcherCtx); err != nil {
		t.Fatalf("failed to start dispatcher: %v", err)
	}
	defer dispatcher.Stop()

	t.Run("cron job creates and executes job instances", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "cron test"}`)

		// Create a cron job that should have run 1 minute ago
		pastTime := time.Now().Add(-1 * time.Minute)
		cronJob := &cron.CronJob{
			ID:         "test-cron-1",
			Name:       "Test Every 5 Minutes",
			CronExpr:   "*/5 * * * *",
			JobType:    "cron-echo",
			Payload:    payload,
			Priority:   10,
			MaxRetries: 3,
			Timeout:    60 * time.Second,
			Enabled:    true,
			NextRun:    &pastTime,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := persQueue.CreateCronJob(ctx, cronJob)
		if err != nil {
			t.Fatalf("failed to create cron job: %v", err)
		}

		// Create scheduler and process due jobs
		scheduler := cron.NewScheduler(persQueue, logger)
		err = scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("failed to process due jobs: %v", err)
		}

		// Wait for job to be processed
		time.Sleep(2 * time.Second)

		if executionCount != 1 {
			t.Errorf("expected execution count 1, got %d", executionCount)
		}

		// Verify cron job was updated
		updated, exists := persQueue.GetCronJob(ctx, "test-cron-1")
		if !exists {
			t.Fatal("expected cron job to exist")
		}

		if updated.NextRun == nil {
			t.Error("expected next run to be scheduled")
		}

		if updated.NextRun.Before(time.Now()) {
			t.Error("expected next run to be in the future")
		}

		if updated.LastRun == nil {
			t.Error("expected last run to be set")
		}
	})

	t.Run("disabled cron jobs are not executed", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "disabled test"}`)
		pastTime := time.Now().Add(-1 * time.Minute)

		disabledCronJob := &cron.CronJob{
			ID:         "test-cron-disabled",
			Name:       "Disabled Cron",
			CronExpr:   "*/5 * * * *",
			JobType:    "cron-echo",
			Payload:    payload,
			Enabled:    false,
			NextRun:    &pastTime,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := persQueue.CreateCronJob(ctx, disabledCronJob)
		if err != nil {
			t.Fatalf("failed to create disabled cron job: %v", err)
		}

		beforeCount := executionCount

		scheduler := cron.NewScheduler(persQueue, logger)
		err = scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("failed to process due jobs: %v", err)
		}

		time.Sleep(1 * time.Second)

		if executionCount != beforeCount {
			t.Errorf("disabled cron job should not execute, count changed from %d to %d", beforeCount, executionCount)
		}
	})

	t.Run("cron job respects priority and retry settings", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "priority test"}`)
		pastTime := time.Now().Add(-1 * time.Minute)

		highPriorityCron := &cron.CronJob{
			ID:         "test-cron-priority",
			Name:       "High Priority Cron",
			CronExpr:   "*/5 * * * *",
			JobType:    "cron-echo",
			Payload:    payload,
			Priority:   100,
			MaxRetries: 10,
			Timeout:    2 * time.Minute,
			Enabled:    true,
			NextRun:    &pastTime,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := persQueue.CreateCronJob(ctx, highPriorityCron)
		if err != nil {
			t.Fatalf("failed to create high priority cron job: %v", err)
		}

		scheduler := cron.NewScheduler(persQueue, logger)
		err = scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("failed to process due jobs: %v", err)
		}

		// Find the created job
		jobs := persQueue.ListJobsByStatus(ctx, "pending")

		var createdJob *queue.JobRecord
		for _, job := range jobs {
			if job.Type == "cron-echo" && job.Priority == 100 {
				createdJob = job
				break
			}
		}

		if createdJob == nil {
			t.Fatal("expected to find created job with priority 100")
		}

		if createdJob.MaxRetries != 10 {
			t.Errorf("expected max retries 10, got %d", createdJob.MaxRetries)
		}

		if createdJob.Timeout != 2*time.Minute {
			t.Errorf("expected timeout 2m, got %v", createdJob.Timeout)
		}
	})

	t.Run("multiple cron jobs execute independently", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "multi test"}`)
		pastTime := time.Now().Add(-1 * time.Minute)

		cron1 := &cron.CronJob{
			ID:         "multi-cron-1",
			Name:       "Multi Cron 1",
			CronExpr:   "*/5 * * * *",
			JobType:    "cron-echo",
			Payload:    payload,
			Enabled:    true,
			NextRun:    &pastTime,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		cron2 := &cron.CronJob{
			ID:         "multi-cron-2",
			Name:       "Multi Cron 2",
			CronExpr:   "*/10 * * * *",
			JobType:    "cron-echo",
			Payload:    payload,
			Enabled:    true,
			NextRun:    &pastTime,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		persQueue.CreateCronJob(ctx, cron1)
		persQueue.CreateCronJob(ctx, cron2)

		beforeCount := executionCount

		scheduler := cron.NewScheduler(persQueue, logger)
		err = scheduler.ProcessDueJobs(ctx)
		if err != nil {
			t.Fatalf("failed to process due jobs: %v", err)
		}

		time.Sleep(2 * time.Second)

		expectedCount := beforeCount + 2
		if executionCount != expectedCount {
			t.Errorf("expected %d executions, got %d", expectedCount, executionCount)
		}
	})
}
