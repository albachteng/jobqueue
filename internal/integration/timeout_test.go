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

func TestTimeout_EndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	persQueue, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	registry := jobs.NewRegistry()

	t.Run("job exceeding timeout is cancelled and retried", func(t *testing.T) {
		var attempts atomic.Int32

		registry.MustRegister("slow-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			attempts.Add(1)
			<-ctx.Done()
			return ctx.Err()
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

		payload := json.RawMessage(`{"test":"timeout"}`)
		env, err := jobs.NewEnvelope("slow-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		env.Timeout = 50 * time.Millisecond
		env.MaxRetries = 2

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued timeout job %s with 50ms timeout", jobID)

		time.Sleep(1 * time.Second)

		cancel()
		dispatcher.Stop()
		time.Sleep(100 * time.Millisecond)

		totalAttempts := attempts.Load()
		if totalAttempts < 1 {
			t.Errorf("expected at least 1 attempt, got %d", totalAttempts)
		}

		if totalAttempts > 3 {
			t.Errorf("expected at most 3 attempts (initial + 2 retries), got %d", totalAttempts)
		}

		t.Logf("Job attempted %d times", totalAttempts)

		record, exists := persQueue.GetJob(context.Background(), jobID)
		if !exists {
			t.Fatal("job not found in database")
		}

		if record.Status != "dlq" {
			t.Errorf("expected job in DLQ, got status: %s", record.Status)
		}

		if record.Attempts != 3 {
			t.Errorf("expected 3 attempts in DLQ record, got %d", record.Attempts)
		}

		t.Logf("Job correctly moved to DLQ after timeout retries exhausted")
	})

	t.Run("job completing within timeout succeeds", func(t *testing.T) {
		var completed atomic.Bool

		registry.MustRegister("fast-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			time.Sleep(20 * time.Millisecond)
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

		t.Cleanup(func() {
			cancel()
			dispatcher.Stop()
		})

		payload := json.RawMessage(`{"test":"fast"}`)
		env, err := jobs.NewEnvelope("fast-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		env.Timeout = 100 * time.Millisecond

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued fast job %s with 100ms timeout", jobID)

		time.Sleep(200 * time.Millisecond)

		if !completed.Load() {
			t.Error("job should have completed successfully")
		}

		record, exists := persQueue.GetJob(context.Background(), jobID)
		if !exists {
			t.Fatal("job not found in database")
		}

		if record.Status != "completed" {
			t.Errorf("expected completed status, got: %s", record.Status)
		}

		t.Logf("Job completed successfully within timeout")
	})

	t.Run("job without timeout runs without time limit", func(t *testing.T) {
		var completed atomic.Bool

		registry.MustRegister("unlimited-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			time.Sleep(150 * time.Millisecond)
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

		t.Cleanup(func() {
			cancel()
			dispatcher.Stop()
		})

		payload := json.RawMessage(`{"test":"unlimited"}`)
		env, err := jobs.NewEnvelope("unlimited-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued job %s with no timeout", jobID)

		time.Sleep(300 * time.Millisecond)

		if !completed.Load() {
			t.Error("job should have completed without timeout interference")
		}

		record, exists := persQueue.GetJob(context.Background(), jobID)
		if !exists {
			t.Fatal("job not found in database")
		}

		if record.Status != "completed" {
			t.Errorf("expected completed status, got: %s", record.Status)
		}

		t.Logf("Job with no timeout completed successfully")
	})
}
