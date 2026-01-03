package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/worker"
)

// pollJobStatus polls the job status until it matches expectedStatus or times out
func pollJobStatus(t *testing.T, q queue.PersistentQueue, jobID jobs.JobID, expectedStatus string, timeout time.Duration) *queue.JobRecord {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		record, exists := q.GetJob(context.Background(), jobID)
		if exists && record.Status == expectedStatus {
			return record
		}
		time.Sleep(50 * time.Millisecond)
	}

	record, exists := q.GetJob(context.Background(), jobID)
	if !exists {
		t.Fatalf("job %s not found after %v", jobID, timeout)
	}
	t.Fatalf("job %s did not reach status %q within %v, current status: %q", jobID, expectedStatus, timeout, record.Status)
	return nil
}

// pollAttempts polls until attempts counter reaches expected value or times out
func pollAttempts(t *testing.T, counter *atomic.Int32, expected int32, timeout time.Duration) int32 {
	t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		current := counter.Load()
		if current == expected {
			return current
		}
		time.Sleep(50 * time.Millisecond)
	}

	return counter.Load()
}

func TestMaxRetries_EndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/maxretries_test.db"
	persQueue, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	registry := jobs.NewRegistry()

	t.Run("job with max_retries=3 retries exactly 3 times", func(t *testing.T) {
		var attempts atomic.Int32

		registry.MustRegister("retry-3-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			attempts.Add(1)
			return errors.New("intentional failure")
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		payload := json.RawMessage(`{"test":"retry-3"}`)
		env, err := jobs.NewEnvelope("retry-3-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		env.MaxRetries = 3

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued job %s with MaxRetries=3", jobID)

		// Wait for job to fail and be moved to DLQ
		record := pollJobStatus(t, persQueue, jobID, "dlq", 5*time.Second)

		totalAttempts := attempts.Load()
		expectedAttempts := int32(4) // 1 initial + 3 retries

		if totalAttempts != expectedAttempts {
			t.Errorf("expected %d attempts (1 initial + 3 retries), got %d", expectedAttempts, totalAttempts)
		}

		if record.Attempts != 4 {
			t.Errorf("expected 4 attempts recorded, got %d", record.Attempts)
		}

		t.Logf("Job correctly attempted %d times and moved to DLQ", totalAttempts)
	})

	t.Run("job with max_retries=0 fails immediately without retries", func(t *testing.T) {
		var attempts atomic.Int32

		registry.MustRegister("no-retry-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			attempts.Add(1)
			return errors.New("intentional failure")
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

		payload := json.RawMessage(`{"test":"no-retry"}`)
		env, err := jobs.NewEnvelope("no-retry-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		env.MaxRetries = 0

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued job %s with MaxRetries=0", jobID)

		// Wait for job to fail
		record := pollJobStatus(t, persQueue, jobID, "dlq", 3*time.Second)

		totalAttempts := attempts.Load()
		expectedAttempts := int32(1) // Only 1 attempt, no retries

		if totalAttempts != expectedAttempts {
			t.Errorf("expected %d attempt (no retries), got %d", expectedAttempts, totalAttempts)
		}

		if record.Attempts != 1 {
			t.Errorf("expected 1 attempt recorded, got %d", record.Attempts)
		}

		t.Logf("Job correctly attempted once without retries and moved to DLQ")
	})

	t.Run("job with max_retries=1 succeeds on retry", func(t *testing.T) {
		var attempts atomic.Int32

		registry.MustRegister("succeed-on-retry-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			currentAttempt := attempts.Add(1)
			if currentAttempt == 1 {
				return errors.New("fail first time")
			}
			return nil // Succeed on retry
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

		payload := json.RawMessage(`{"test":"succeed-on-retry"}`)
		env, err := jobs.NewEnvelope("succeed-on-retry-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		env.MaxRetries = 1

		if err := persQueue.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		jobID := env.ID
		t.Logf("Enqueued job %s with MaxRetries=1", jobID)

		// Wait for job to complete
		record := pollJobStatus(t, persQueue, jobID, "completed", 3*time.Second)

		totalAttempts := attempts.Load()
		expectedAttempts := int32(2) // 1 initial + 1 retry

		if totalAttempts != expectedAttempts {
			t.Errorf("expected %d attempts, got %d", expectedAttempts, totalAttempts)
		}

		if record.Attempts != 2 {
			t.Errorf("expected 2 attempts recorded, got %d", record.Attempts)
		}

		t.Logf("Job correctly retried once and succeeded")
	})

	t.Run("different jobs with different max_retries values", func(t *testing.T) {
		var attemptsJobA atomic.Int32
		var attemptsJobB atomic.Int32

		registry.MustRegister("job-a", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			attemptsJobA.Add(1)
			return errors.New("fail")
		}))

		registry.MustRegister("job-b", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			attemptsJobB.Add(1)
			return errors.New("fail")
		}))

		logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelError,
		}))

		dispatcher := worker.NewDispatcher(persQueue, 2, registry, nil, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := dispatcher.Start(ctx); err != nil {
			t.Fatalf("failed to start dispatcher: %v", err)
		}
		defer dispatcher.Stop()

		// Enqueue job A with max_retries=2
		envA, _ := jobs.NewEnvelope("job-a", json.RawMessage(`{}`))
		envA.MaxRetries = 2
		persQueue.Enqueue(ctx, envA)
		t.Logf("Enqueued job A with MaxRetries=2")

		// Enqueue job B with max_retries=5
		envB, _ := jobs.NewEnvelope("job-b", json.RawMessage(`{}`))
		envB.MaxRetries = 5
		persQueue.Enqueue(ctx, envB)
		t.Logf("Enqueued job B with MaxRetries=5")

		// Wait for both jobs to reach DLQ
		pollJobStatus(t, persQueue, envA.ID, "dlq", 5*time.Second)
		pollJobStatus(t, persQueue, envB.ID, "dlq", 5*time.Second)

		attemptsA := attemptsJobA.Load()
		attemptsB := attemptsJobB.Load()

		if attemptsA != 3 { // 1 initial + 2 retries
			t.Errorf("job A: expected 3 attempts, got %d", attemptsA)
		}

		if attemptsB != 6 { // 1 initial + 5 retries
			t.Errorf("job B: expected 6 attempts, got %d", attemptsB)
		}

		t.Logf("Job A attempted %d times, Job B attempted %d times", attemptsA, attemptsB)
	})
}
