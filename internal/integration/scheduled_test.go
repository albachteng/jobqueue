package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/worker"
)

func TestScheduled_EndToEnd(t *testing.T) {
	dbPath := t.TempDir() + "/test.db"
	persQueue, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	registry := jobs.NewRegistry()
	processedJobs := make(chan jobs.JobID, 10)
	registry.MustRegister("scheduled-job", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		processedJobs <- env.ID
		return nil
	}))

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError, // Only show errors to keep test output clean
	}))

	dispatcher := worker.NewDispatcher(persQueue, 1, registry, nil, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := dispatcher.Start(ctx); err != nil {
		t.Fatalf("failed to start dispatcher: %v", err)
	}
	// Ensure proper cleanup
	t.Cleanup(func() {
		cancel()
		dispatcher.Stop()
	})

	// Test 1: Job scheduled in the future should not be processed immediately
	t.Run("job scheduled in future not processed immediately", func(t *testing.T) {
		payload := json.RawMessage(`{"test":"future"}`)
		futureEnv, err := jobs.NewEnvelope("scheduled-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		futureTime := time.Now().Add(200 * time.Millisecond)
		futureEnv.ScheduledAt = &futureTime

		if err := persQueue.Enqueue(ctx, futureEnv); err != nil {
			t.Fatalf("failed to enqueue future job: %v", err)
		}

		futureJobID := futureEnv.ID
		t.Logf("Enqueued future job %s scheduled for %v", futureJobID, futureTime)

		// Wait 50ms and verify job hasn't been processed yet
		select {
		case processedID := <-processedJobs:
			t.Errorf("future job %s was processed too early (got %s)", futureJobID, processedID)
		case <-time.After(50 * time.Millisecond):
			t.Logf("Future job correctly not processed yet")
		}

		// Wait for scheduled time and verify job gets processed
		waitTime := time.Until(futureTime) + 100*time.Millisecond
		t.Logf("Waiting %v for job to become ready", waitTime)

		select {
		case processedID := <-processedJobs:
			if processedID != futureJobID {
				t.Errorf("expected job %s, got %s", futureJobID, processedID)
			}
			t.Logf("Future job %s processed at correct time", processedID)
		case <-time.After(waitTime):
			t.Errorf("future job was not processed after scheduled time")
		}
	})

	// Test 2: Immediate job (nil ScheduledAt) should be processed right away
	t.Run("immediate job processed immediately", func(t *testing.T) {
		payload := json.RawMessage(`{"test":"immediate"}`)
		immediateEnv, err := jobs.NewEnvelope("scheduled-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}
		// ScheduledAt is nil by default (immediate job)

		if err := persQueue.Enqueue(ctx, immediateEnv); err != nil {
			t.Fatalf("failed to enqueue immediate job: %v", err)
		}

		immediateJobID := immediateEnv.ID
		t.Logf("Enqueued immediate job %s", immediateJobID)

		select {
		case processedID := <-processedJobs:
			if processedID != immediateJobID {
				t.Errorf("expected job %s, got %s", immediateJobID, processedID)
			}
			t.Logf("Immediate job %s processed quickly", processedID)
		case <-time.After(500 * time.Millisecond):
			t.Errorf("immediate job was not processed within 500ms")
		}
	})

	// Test 3: Immediate job should be processed before future job
	t.Run("immediate job processed before future job", func(t *testing.T) {
		// Enqueue a job scheduled 150ms in the future
		futurePayload := json.RawMessage(`{"test":"future2"}`)
		futureEnv2, err := jobs.NewEnvelope("scheduled-job", futurePayload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}
		futureTime2 := time.Now().Add(150 * time.Millisecond)
		futureEnv2.ScheduledAt = &futureTime2

		if err := persQueue.Enqueue(ctx, futureEnv2); err != nil {
			t.Fatalf("failed to enqueue future job: %v", err)
		}
		futureJobID2 := futureEnv2.ID

		// Immediately enqueue an immediate job
		immediatePayload := json.RawMessage(`{"test":"immediate2"}`)
		immediateEnv2, err := jobs.NewEnvelope("scheduled-job", immediatePayload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		if err := persQueue.Enqueue(ctx, immediateEnv2); err != nil {
			t.Fatalf("failed to enqueue immediate job: %v", err)
		}
		immediateJobID2 := immediateEnv2.ID

		t.Logf("Enqueued future job %s and immediate job %s", futureJobID2, immediateJobID2)

		// The immediate job should be processed first
		select {
		case processedID := <-processedJobs:
			if processedID != immediateJobID2 {
				t.Errorf("expected immediate job %s first, got %s", immediateJobID2, processedID)
			}
			t.Logf("Immediate job %s processed first (correct)", processedID)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("no job processed within 500ms")
		}

		// Then the future job should be processed
		select {
		case processedID := <-processedJobs:
			if processedID != futureJobID2 {
				t.Errorf("expected future job %s second, got %s", futureJobID2, processedID)
			}
			t.Logf("Future job %s processed second (correct)", processedID)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("future job not processed after scheduled time")
		}
	})

	// Test 4: Job scheduled in the past should be processed immediately
	t.Run("job scheduled in past processed immediately", func(t *testing.T) {
		payload := json.RawMessage(`{"test":"past"}`)
		pastEnv, err := jobs.NewEnvelope("scheduled-job", payload)
		if err != nil {
			t.Fatalf("failed to create envelope: %v", err)
		}

		pastTime := time.Now().Add(-1 * time.Hour)
		pastEnv.ScheduledAt = &pastTime

		if err := persQueue.Enqueue(ctx, pastEnv); err != nil {
			t.Fatalf("failed to enqueue past job: %v", err)
		}

		pastJobID := pastEnv.ID
		t.Logf("Enqueued past job %s scheduled for %v", pastJobID, pastTime)

		select {
		case processedID := <-processedJobs:
			if processedID != pastJobID {
				t.Errorf("expected job %s, got %s", pastJobID, processedID)
			}
			t.Logf("Past job %s processed immediately", processedID)
		case <-time.After(500 * time.Millisecond):
			t.Errorf("past job was not processed within 500ms")
		}
	})

	t.Logf("End-to-end scheduled jobs test completed successfully")
}
