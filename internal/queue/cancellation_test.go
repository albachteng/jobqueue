package queue

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/albachteng/jobqueue/internal/jobs"
)

func TestSQLiteQueue_CancelJob(t *testing.T) {
	t.Run("cancels pending job", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_pending.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", json.RawMessage(`{}`))

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Verify job is pending
		record, exists := q.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after enqueue")
		}
		if record.Status != "pending" {
			t.Errorf("expected pending status, got %s", record.Status)
		}

		// Cancel the job
		if err := q.CancelJob(ctx, env.ID); err != nil {
			t.Fatalf("failed to cancel job: %v", err)
		}

		// Verify job is cancelled
		record, exists = q.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after cancellation")
		}
		if record.Status != "cancelled" {
			t.Errorf("expected cancelled status, got %s", record.Status)
		}

		// Verify cancelled job is not dequeued
		_, err = q.Dequeue(ctx)
		if err != ErrEmptyQueue {
			t.Errorf("expected ErrEmptyQueue when dequeuing cancelled job, got: %v", err)
		}
	})

	t.Run("cannot cancel completed job", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_completed.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", json.RawMessage(`{}`))

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Complete the job
		if err := q.CompleteJob(ctx, env.ID, 1); err != nil {
			t.Fatalf("failed to complete job: %v", err)
		}

		// Try to cancel completed job
		err = q.CancelJob(ctx, env.ID)
		if err == nil {
			t.Error("expected error when cancelling completed job")
		}
		if err != ErrJobNotCancellable {
			t.Errorf("expected ErrJobNotCancellable, got: %v", err)
		}

		// Verify job is still completed
		record, exists := q.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after cancel attempt")
		}
		if record.Status != "completed" {
			t.Errorf("expected completed status, got %s", record.Status)
		}
	})

	t.Run("cannot cancel processing job", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_processing.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", json.RawMessage(`{}`))

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Dequeue to set status to processing
		_, err = q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}

		// Verify job is processing
		record, exists := q.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after dequeue")
		}
		if record.Status != "processing" {
			t.Errorf("expected processing status, got %s", record.Status)
		}

		// Try to cancel the processing job
		err = q.CancelJob(ctx, env.ID)
		if err == nil {
			t.Error("expected error when cancelling processing job")
		}
		if err != ErrJobNotCancellable {
			t.Errorf("expected ErrJobNotCancellable, got: %v", err)
		}

		// Verify job is still processing
		record, exists = q.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after cancel attempt")
		}
		if record.Status != "processing" {
			t.Errorf("expected processing status to remain, got %s", record.Status)
		}
	})

	t.Run("cannot cancel job in DLQ", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_dlq.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", json.RawMessage(`{}`))

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		// Move job to DLQ
		if err := q.MoveToDLQ(ctx, env, "test error"); err != nil {
			t.Fatalf("failed to move to DLQ: %v", err)
		}

		// Try to cancel DLQ job
		err = q.CancelJob(ctx, env.ID)
		if err == nil {
			t.Error("expected error when cancelling DLQ job")
		}
		if err != ErrJobNotCancellable {
			t.Errorf("expected ErrJobNotCancellable, got: %v", err)
		}

		// Verify job is still in DLQ
		record, exists := q.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after cancel attempt")
		}
		if record.Status != "dlq" {
			t.Errorf("expected dlq status, got %s", record.Status)
		}
	})

	t.Run("returns error for non-existent job", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_nonexistent.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()
		nonExistentID := jobs.JobID("non-existent-job-id")

		err = q.CancelJob(ctx, nonExistentID)
		if err == nil {
			t.Error("expected error when cancelling non-existent job")
		}
		if err != ErrJobNotFound {
			t.Errorf("expected ErrJobNotFound, got: %v", err)
		}
	})

	t.Run("cancelled job is excluded from dequeue", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_dequeue.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		// Enqueue two jobs
		env1, _ := jobs.NewEnvelope("test", json.RawMessage(`{"job":1}`))
		env2, _ := jobs.NewEnvelope("test", json.RawMessage(`{"job":2}`))

		if err := q.Enqueue(ctx, env1); err != nil {
			t.Fatalf("failed to enqueue job 1: %v", err)
		}
		if err := q.Enqueue(ctx, env2); err != nil {
			t.Fatalf("failed to enqueue job 2: %v", err)
		}

		// Cancel first job
		if err := q.CancelJob(ctx, env1.ID); err != nil {
			t.Fatalf("failed to cancel job 1: %v", err)
		}

		// Dequeue should return second job, not first
		dequeued, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}

		if dequeued.ID != env2.ID {
			t.Errorf("expected to dequeue job 2 (ID: %s), got job with ID: %s", env2.ID, dequeued.ID)
		}

		// Verify we can get cancelled job info
		record, exists := q.GetJob(ctx, env1.ID)
		if !exists {
			t.Fatal("cancelled job not found")
		}
		if record.Status != "cancelled" {
			t.Errorf("expected cancelled status, got %s", record.Status)
		}
	})

	t.Run("list cancelled jobs separately from pending", func(t *testing.T) {
		dbPath := t.TempDir() + "/cancel_list.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		// Enqueue three jobs
		env1, _ := jobs.NewEnvelope("test", json.RawMessage(`{"job":1}`))
		env2, _ := jobs.NewEnvelope("test", json.RawMessage(`{"job":2}`))
		env3, _ := jobs.NewEnvelope("test", json.RawMessage(`{"job":3}`))

		q.Enqueue(ctx, env1)
		q.Enqueue(ctx, env2)
		q.Enqueue(ctx, env3)

		// Cancel one job
		if err := q.CancelJob(ctx, env2.ID); err != nil {
			t.Fatalf("failed to cancel job: %v", err)
		}

		// List pending jobs should not include cancelled
		pendingJobs := q.ListJobsByStatus(ctx, "pending")
		if len(pendingJobs) != 2 {
			t.Errorf("expected 2 pending jobs, got %d", len(pendingJobs))
		}

		// List cancelled jobs
		cancelledJobs := q.ListJobsByStatus(ctx, "cancelled")
		if len(cancelledJobs) != 1 {
			t.Errorf("expected 1 cancelled job, got %d", len(cancelledJobs))
		}

		if len(cancelledJobs) > 0 && cancelledJobs[0].Envelope.ID != env2.ID {
			t.Errorf("expected cancelled job ID %s, got %s", env2.ID, cancelledJobs[0].Envelope.ID)
		}
	})
}
