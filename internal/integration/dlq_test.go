package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/worker"
)

func TestDLQ_EndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	persQueue, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	registry := jobs.NewRegistry()
	failCount := 0
	registry.MustRegister("failing-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		failCount++
		return errors.New("intentional failure")
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // Only show errors to keep test output clean
	}))

	dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := dispatcher.Start(ctx); err != nil {
		t.Fatalf("failed to start dispatcher: %v", err)
	}
	defer dispatcher.Stop()

	payload := json.RawMessage(`{"test":"data"}`)
	envelope, err := jobs.NewEnvelope("failing-job", payload)
	if err != nil {
		t.Fatalf("failed to create envelope: %v", err)
	}
	envelope.MaxRetries = 2 // Will attempt 3 times total (initial + 2 retries)

	if err := persQueue.Enqueue(ctx, envelope); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}

	jobID := envelope.ID
	t.Logf("Enqueued job %s", jobID)

	// Poll for job to be moved to DLQ (with timeout)
	// It should fail on attempts 1, 2, and 3, then move to DLQ
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	var dlqJobs []*queue.JobRecord
	for {
		select {
		case <-timeout:
			t.Fatalf("timeout waiting for job to move to DLQ, got %d jobs", len(dlqJobs))
		case <-ticker.C:
			dlqJobs = persQueue.ListDLQJobs(ctx)
			if len(dlqJobs) == 1 {
				goto jobInDLQ
			}
		}
	}
jobInDLQ:

	if len(dlqJobs) != 1 {
		t.Fatalf("expected 1 job in DLQ, got %d", len(dlqJobs))
	}

	if dlqJobs[0].ID != jobID {
		t.Errorf("expected job ID %s in DLQ, got %s", jobID, dlqJobs[0].ID)
	}

	if dlqJobs[0].Status != "dlq" {
		t.Errorf("expected status 'dlq', got %s", dlqJobs[0].Status)
	}

	if dlqJobs[0].Attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", dlqJobs[0].Attempts)
	}

	if dlqJobs[0].LastError == "" {
		t.Error("expected error message to be preserved")
	}

	t.Logf("Job successfully moved to DLQ after %d attempts", dlqJobs[0].Attempts)

	var payloadData map[string]string
	if err := json.Unmarshal(dlqJobs[0].Payload, &payloadData); err != nil {
		t.Errorf("failed to unmarshal payload: %v", err)
	}
	if payloadData["test"] != "data" {
		t.Errorf("payload not preserved, got %v", payloadData)
	}

	// Stop the dispatcher before requeueing to avoid immediate processing
	cancel()
	dispatcher.Stop()
	time.Sleep(100 * time.Millisecond) // Give time for workers to stop

	// Requeue the job from DLQ (use fresh context since we cancelled the dispatcher context)
	freshCtx := context.Background()
	if err := persQueue.RequeueDLQJob(freshCtx, jobID); err != nil {
		t.Fatalf("failed to requeue job from DLQ: %v", err)
	}

	t.Logf("Job requeued from DLQ")

	// Verify job is back in pending status
	record, exists := persQueue.GetJob(context.Background(), jobID)
	if !exists {
		t.Fatal("job not found after requeue")
	}

	if record.Status != "pending" {
		t.Errorf("expected status 'pending' after requeue, got %s", record.Status)
	}

	if record.Attempts != 0 {
		t.Errorf("expected attempts to be reset to 0, got %d", record.Attempts)
	}

	if record.LastError != "" {
		t.Errorf("expected error to be cleared, got %s", record.LastError)
	}

	t.Logf("Job successfully requeued with reset attempts")

	// Verify DLQ is now empty
	dlqJobs = persQueue.ListDLQJobs(context.Background())
	if len(dlqJobs) != 0 {
		t.Errorf("expected DLQ to be empty after requeue, got %d jobs", len(dlqJobs))
	}

	t.Logf("End-to-end DLQ test completed successfully")
}
