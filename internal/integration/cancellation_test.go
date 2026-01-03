package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/worker"
)

func TestCancellation_EndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	persQueue, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	registry := jobs.NewRegistry()

	t.Run("cancel pending job before it starts processing", func(t *testing.T) {
		registry.MustRegister("pending-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			t.Error("job should not have been executed after cancellation")
			return nil
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		// Create dispatcher with 0 workers so job stays pending
		dispatcher := worker.NewDispatcher(persQueue, 0, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		payload := json.RawMessage(`{"test":"pending"}`)
		env, err := jobs.NewEnvelope("pending-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued pending job %s", jobID)

		// Verify job is pending
		record, exists := persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found in database")
		}
		if record.Status != "pending" {
			t.Errorf("expected pending status, got: %s", record.Status)
		}

		// Cancel the job
		if err := persQueue.CancelJob(ctx, jobID); err != nil {
			t.Fatalf("failed to cancel job: %v", err)
		}

		// Verify job is cancelled
		record, exists = persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found after cancellation")
		}
		if record.Status != "cancelled" {
			t.Errorf("expected cancelled status, got: %s", record.Status)
		}

		t.Logf("Pending job successfully cancelled")
	})

	t.Run("cannot cancel processing job", func(t *testing.T) {
		var started atomic.Bool
		var completed atomic.Bool

		registry.MustRegister("quick-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			started.Store(true)
			time.Sleep(100 * time.Millisecond)
			completed.Store(true)
			return nil
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		payload := json.RawMessage(`{"test":"processing"}`)
		env, err := jobs.NewEnvelope("quick-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued quick job %s", jobID)

		// Wait for job to start processing
		for i := 0; i < 50; i++ {
			if started.Load() {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if !started.Load() {
			t.Fatal("job did not start processing")
		}

		// Verify job is processing
		record, exists := persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found in database")
		}
		if record.Status != "processing" {
			t.Errorf("expected processing status, got: %s", record.Status)
		}

		// Try to cancel the job while it's processing
		err = persQueue.CancelJob(ctx, jobID)
		if err == nil {
			t.Error("expected error when cancelling processing job")
		}
		if err != queue.ErrJobNotCancellable {
			t.Errorf("expected ErrJobNotCancellable, got: %v", err)
		}

		// Wait for job to complete
		time.Sleep(200 * time.Millisecond)

		if !completed.Load() {
			t.Error("job should have completed normally")
		}

		// Verify job completed successfully
		record, exists = persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found after completion")
		}
		if record.Status != "completed" {
			t.Errorf("expected completed status, got: %s", record.Status)
		}

		t.Logf("Processing job correctly rejected cancellation and completed normally")
	})

	t.Run("cancel completed job returns error", func(t *testing.T) {
		registry.MustRegister("completed-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		payload := json.RawMessage(`{"test":"completed"}`)
		env, err := jobs.NewEnvelope("completed-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued job %s", jobID)

		// Wait for job to complete
		time.Sleep(200 * time.Millisecond)

		// Verify job is completed
		record, exists := persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found in database")
		}
		if record.Status != "completed" {
			t.Errorf("expected completed status, got: %s", record.Status)
		}

		// Try to cancel completed job
		err = persQueue.CancelJob(ctx, jobID)
		if err == nil {
			t.Error("expected error when cancelling completed job")
		}
		if err != queue.ErrJobNotCancellable {
			t.Errorf("expected ErrJobNotCancellable, got: %v", err)
		}

		// Verify job is still completed
		record, exists = persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found after cancel attempt")
		}
		if record.Status != "completed" {
			t.Errorf("expected completed status to remain, got: %s", record.Status)
		}

		t.Logf("Completed job correctly rejected cancellation")
	})

	t.Run("cancel non-existent job returns error", func(t *testing.T) {
		ctx := context.Background()
		nonExistentID := jobs.JobID("non-existent-job-id")

		err := persQueue.CancelJob(ctx, nonExistentID)
		if err == nil {
			t.Error("expected error when cancelling non-existent job")
		}
		if err != queue.ErrJobNotFound {
			t.Errorf("expected ErrJobNotFound, got: %v", err)
		}

		t.Logf("Non-existent job correctly returned error")
	})

	t.Run("cancel scheduled job before it runs", func(t *testing.T) {
		registry.MustRegister("scheduled-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			t.Error("scheduled job should not have been executed after cancellation")
			return nil
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		// Schedule job for 10 seconds in the future
		futureTime := time.Now().Add(10 * time.Second)
		payload := json.RawMessage(`{"test":"scheduled"}`)
		env, err := jobs.NewEnvelope("scheduled-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}
		env.ScheduledAt = &futureTime

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued scheduled job %s for %s", jobID, futureTime)

		// Verify job is pending
		record, exists := persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found in database")
		}
		if record.Status != "pending" {
			t.Errorf("expected pending status, got: %s", record.Status)
		}

		// Cancel the scheduled job
		if err := persQueue.CancelJob(ctx, jobID); err != nil {
			t.Fatalf("failed to cancel scheduled job: %v", err)
		}

		// Verify job is cancelled
		record, exists = persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found after cancellation")
		}
		if record.Status != "cancelled" {
			t.Errorf("expected cancelled status, got: %s", record.Status)
		}

		// Wait to ensure job doesn't execute
		time.Sleep(200 * time.Millisecond)

		t.Logf("Scheduled job successfully cancelled")
	})

	t.Run("cancel failed job returns error", func(t *testing.T) {
		registry.MustRegister("failing-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return jobs.ErrJobFailed
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		payload := json.RawMessage(`{"test":"failed"}`)
		env, err := jobs.NewEnvelope("failing-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued failing job %s", jobID)

		// Wait for job to fail and move to DLQ
		time.Sleep(300 * time.Millisecond)

		// Verify job is in DLQ
		record, exists := persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Fatal("job not found in database")
		}
		if record.Status != "dlq" {
			t.Errorf("expected dlq status, got: %s", record.Status)
		}

		// Try to cancel failed job
		err = persQueue.CancelJob(ctx, jobID)
		if err == nil {
			t.Error("expected error when cancelling failed job in DLQ")
		}
		if err != queue.ErrJobNotCancellable {
			t.Errorf("expected ErrJobNotCancellable, got: %v", err)
		}

		t.Logf("Failed job correctly rejected cancellation")
	})
}
