package queue

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
)

func TestCronJobStorage(t *testing.T) {
	dbPath := t.TempDir() + "/cronjob_test.db"
	q, err := NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	ctx := context.Background()

	t.Run("create cron job", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		cronJob := &cron.CronJob{
			ID:         "cron-1",
			Name:       "Test Cron Job",
			CronExpr:   "*/5 * * * *",
			JobType:    "echo",
			Payload:    payload,
			Priority:   5,
			MaxRetries: 3,
			Timeout:    30 * time.Second,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := q.CreateCronJob(ctx, cronJob)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		retrieved, exists := q.GetCronJob(ctx, cronJob.ID)
		if !exists {
			t.Fatal("expected cron job to exist")
		}

		if retrieved.Name != cronJob.Name {
			t.Errorf("expected name %q, got %q", cronJob.Name, retrieved.Name)
		}
		if retrieved.CronExpr != cronJob.CronExpr {
			t.Errorf("expected cron expression %q, got %q", cronJob.CronExpr, retrieved.CronExpr)
		}
		if retrieved.Enabled != cronJob.Enabled {
			t.Errorf("expected enabled %v, got %v", cronJob.Enabled, retrieved.Enabled)
		}
	})

	t.Run("list cron jobs", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		cronJob2 := &cron.CronJob{
			ID:         "cron-2",
			Name:       "Second Cron Job",
			CronExpr:   "0 0 * * *",
			JobType:    "echo",
			Payload:    payload,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := q.CreateCronJob(ctx, cronJob2)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		cronJobs := q.ListCronJobs(ctx)
		if len(cronJobs) < 2 {
			t.Errorf("expected at least 2 cron jobs, got %d", len(cronJobs))
		}
	})

	t.Run("update cron job", func(t *testing.T) {
		cronJob, exists := q.GetCronJob(ctx, "cron-1")
		if !exists {
			t.Fatal("expected cron job to exist")
		}

		cronJob.Name = "Updated Name"
		cronJob.Enabled = false

		err := q.UpdateCronJob(ctx, cronJob)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		updated, exists := q.GetCronJob(ctx, "cron-1")
		if !exists {
			t.Fatal("expected cron job to exist")
		}

		if updated.Name != "Updated Name" {
			t.Errorf("expected name %q, got %q", "Updated Name", updated.Name)
		}
		if updated.Enabled {
			t.Error("expected enabled to be false")
		}
	})

	t.Run("delete cron job", func(t *testing.T) {
		err := q.DeleteCronJob(ctx, "cron-1")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		_, exists := q.GetCronJob(ctx, "cron-1")
		if exists {
			t.Error("expected cron job to be deleted")
		}
	})

	t.Run("get enabled cron jobs", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)

		enabledJob := &cron.CronJob{
			ID:         "cron-enabled",
			Name:       "Enabled Job",
			CronExpr:   "*/10 * * * *",
			JobType:    "echo",
			Payload:    payload,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		disabledJob := &cron.CronJob{
			ID:         "cron-disabled",
			Name:       "Disabled Job",
			CronExpr:   "*/15 * * * *",
			JobType:    "echo",
			Payload:    payload,
			Enabled:    false,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		if err := q.CreateCronJob(ctx, enabledJob); err != nil {
			t.Fatalf("failed to create enabled job: %v", err)
		}
		if err := q.CreateCronJob(ctx, disabledJob); err != nil {
			t.Fatalf("failed to create disabled job: %v", err)
		}

		enabledJobs := q.ListEnabledCronJobs(ctx)

		foundEnabled := false
		for _, job := range enabledJobs {
			if job.ID == "cron-disabled" {
				t.Error("disabled job should not be in enabled list")
			}
			if job.ID == "cron-enabled" {
				foundEnabled = true
			}
		}

		if !foundEnabled {
			t.Error("enabled job should be in enabled list")
		}
	})

	t.Run("update next run time", func(t *testing.T) {
		nextRun := time.Now().Add(5 * time.Minute)

		err := q.UpdateCronJobNextRun(ctx, "cron-enabled", nextRun)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		cronJob, exists := q.GetCronJob(ctx, "cron-enabled")
		if !exists {
			t.Fatal("expected cron job to exist")
		}

		if cronJob.NextRun == nil {
			t.Fatal("expected next run to be set")
		}

		if cronJob.NextRun.Unix() != nextRun.Unix() {
			t.Errorf("expected next run %v, got %v", nextRun, cronJob.NextRun)
		}
	})
}

func TestCronJobValidation(t *testing.T) {
	dbPath := t.TempDir() + "/cronjob_validation_test.db"
	q, err := NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	ctx := context.Background()

	t.Run("reject invalid cron expression", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		cronJob := &cron.CronJob{
			ID:         "cron-invalid",
			Name:       "Invalid Cron",
			CronExpr:   "invalid expression",
			JobType:    "echo",
			Payload:    payload,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := q.CreateCronJob(ctx, cronJob)
		if err == nil {
			t.Error("expected error for invalid cron expression")
		}
	})

	t.Run("reject empty job type", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		cronJob := &cron.CronJob{
			ID:         "cron-no-type",
			Name:       "No Type",
			CronExpr:   "*/5 * * * *",
			JobType:    "",
			Payload:    payload,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := q.CreateCronJob(ctx, cronJob)
		if err == nil {
			t.Error("expected error for empty job type")
		}
	})

	t.Run("reject duplicate ID", func(t *testing.T) {
		payload := json.RawMessage(`{"message": "test"}`)
		cronJob := &cron.CronJob{
			ID:         "cron-duplicate",
			Name:       "First",
			CronExpr:   "*/5 * * * *",
			JobType:    "echo",
			Payload:    payload,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err := q.CreateCronJob(ctx, cronJob)
		if err != nil {
			t.Fatalf("expected no error for first creation, got %v", err)
		}

		duplicate := &cron.CronJob{
			ID:         "cron-duplicate",
			Name:       "Duplicate",
			CronExpr:   "*/10 * * * *",
			JobType:    "echo",
			Payload:    payload,
			Enabled:    true,
			CreatedAt:  time.Now(),
			UpdatedAt:  time.Now(),
		}

		err = q.CreateCronJob(ctx, duplicate)
		if err == nil {
			t.Error("expected error for duplicate ID")
		}
	})
}
