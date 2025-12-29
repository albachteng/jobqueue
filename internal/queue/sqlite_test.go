package queue

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/albachteng/jobqueue/internal/jobs"
)

func TestSQLiteQueue_Enqueue(t *testing.T) {
	t.Run("enqueues job successfully", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		env, _ := jobs.NewEnvelope("test", []byte(`{"key":"value"}`))
		ctx := context.Background()

		if err := q.Enqueue(ctx, env); err != nil {
			t.Errorf("enqueue failed: %v", err)
		}
	})

	t.Run("persists job to disk", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		env, _ := jobs.NewEnvelope("test", []byte(`{"key":"value"}`))
		ctx := context.Background()
		q.Enqueue(ctx, env)
		q.Close()

		// Reopen database and verify job exists
		q2, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q2.Close()

		job, err := q2.Dequeue(ctx)
		if err != nil {
			t.Fatalf("dequeue failed: %v", err)
		}

		if job.ID != env.ID {
			t.Errorf("got job ID %s, want %s", job.ID, env.ID)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		env, _ := jobs.NewEnvelope("test", []byte(`{}`))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		if err := q.Enqueue(ctx, env); err == nil {
			t.Error("expected error with cancelled context")
		}
	})
}

func TestSQLiteQueue_Dequeue(t *testing.T) {
	t.Run("dequeues in FIFO order", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		env1, _ := jobs.NewEnvelope("first", []byte(`{}`))
		env2, _ := jobs.NewEnvelope("second", []byte(`{}`))
		env3, _ := jobs.NewEnvelope("third", []byte(`{}`))

		q.Enqueue(ctx, env1)
		q.Enqueue(ctx, env2)
		q.Enqueue(ctx, env3)

		job1, _ := q.Dequeue(ctx)
		job2, _ := q.Dequeue(ctx)
		job3, _ := q.Dequeue(ctx)

		if job1.ID != env1.ID {
			t.Errorf("first job: got %s, want %s", job1.ID, env1.ID)
		}
		if job2.ID != env2.ID {
			t.Errorf("second job: got %s, want %s", job2.ID, env2.ID)
		}
		if job3.ID != env3.ID {
			t.Errorf("third job: got %s, want %s", job3.ID, env3.ID)
		}
	})

	t.Run("returns ErrEmptyQueue when empty", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		_, err = q.Dequeue(ctx)

		if err != ErrEmptyQueue {
			t.Errorf("got error %v, want %v", err, ErrEmptyQueue)
		}
	})

	t.Run("deserializes job correctly", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		type TestPayload struct {
			Message string `json:"message"`
			Count   int    `json:"count"`
		}
		payload := TestPayload{Message: "hello", Count: 42}
		env, _ := jobs.NewEnvelope("test", payload)
		env.MaxRetries = 5

		q.Enqueue(ctx, env)
		job, _ := q.Dequeue(ctx)

		if job.Type != env.Type {
			t.Errorf("got type %s, want %s", job.Type, env.Type)
		}

		var retrieved TestPayload
		if err := job.UnmarshalPayload(&retrieved); err != nil {
			t.Fatalf("failed to unmarshal payload: %v", err)
		}
		if retrieved.Message != "hello" || retrieved.Count != 42 {
			t.Errorf("got payload %+v, want {Message:hello Count:42}", retrieved)
		}

		if job.MaxRetries != 5 {
			t.Errorf("got max_retries %d, want 5", job.MaxRetries)
		}
	})
}

func TestSQLiteQueue_Concurrency(t *testing.T) {
	t.Run("handles concurrent enqueues", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		done := make(chan struct{})

		// Enqueue 100 jobs concurrently
		for i := 0; i < 100; i++ {
			go func() {
				env, _ := jobs.NewEnvelope("concurrent", []byte(`{}`))
				q.Enqueue(ctx, env)
				done <- struct{}{}
			}()
		}

		// Wait for all enqueues
		for i := 0; i < 100; i++ {
			<-done
		}

		// Verify we can dequeue 100 jobs
		count := 0
		for {
			_, err := q.Dequeue(ctx)
			if err == ErrEmptyQueue {
				break
			}
			count++
		}

		if count != 100 {
			t.Errorf("got %d jobs, want 100", count)
		}
	})
}

func TestSQLiteQueue_JobState(t *testing.T) {
	t.Run("marks job as processing when dequeued", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", []byte(`{}`))
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)

		// Job should not be dequeued again
		_, err = q.Dequeue(ctx)
		if err != ErrEmptyQueue {
			t.Errorf("dequeued job should not be available again, got job instead of ErrEmptyQueue")
		}

		// Verify we can access the job through GetJob
		retrieved, exists := q.GetJob(ctx, job.ID)
		if !exists {
			t.Error("job should still exist in database")
		}
		if retrieved.Status != "processing" {
			t.Errorf("got status %s, want 'processing'", retrieved.Status)
		}
	})

	t.Run("completes job and removes from queue", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", []byte(`{}`))
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		if err := q.CompleteJob(ctx, job.ID); err != nil {
			t.Errorf("CompleteJob failed: %v", err)
		}

		// Job should be marked completed but still queryable
		retrieved, exists := q.GetJob(ctx, job.ID)
		if !exists {
			t.Error("completed job should still exist in database")
		}
		if retrieved.Status != "completed" {
			t.Errorf("got status %s, want 'completed'", retrieved.Status)
		}
	})

	t.Run("fails job with error message", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("test", []byte(`{}`))
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		errMsg := "something went wrong"
		if err := q.FailJob(ctx, job.ID, errMsg); err != nil {
			t.Errorf("FailJob failed: %v", err)
		}

		retrieved, exists := q.GetJob(ctx, job.ID)
		if !exists {
			t.Error("failed job should still exist in database")
		}
		if retrieved.Status != "failed" {
			t.Errorf("got status %s, want 'failed'", retrieved.Status)
		}
		if retrieved.LastError != errMsg {
			t.Errorf("got error %q, want %q", retrieved.LastError, errMsg)
		}
	})
}

func TestSQLiteQueue_JobRecovery(t *testing.T) {
	t.Run("recovers pending jobs on startup", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")

		// First instance: enqueue jobs
		q1, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		ctx := context.Background()
		env1, _ := jobs.NewEnvelope("test1", []byte(`{}`))
		env2, _ := jobs.NewEnvelope("test2", []byte(`{}`))
		q1.Enqueue(ctx, env1)
		q1.Enqueue(ctx, env2)
		q1.Close()

		// Second instance: should recover pending jobs
		q2, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q2.Close()

		job1, err := q2.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue job1: %v", err)
		}
		job2, err := q2.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue job2: %v", err)
		}

		if job1.ID != env1.ID || job2.ID != env2.ID {
			t.Error("jobs not recovered correctly")
		}
	})

	t.Run("resets stuck processing jobs to pending", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")

		// First instance: dequeue job but don't complete
		q1, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("stuck", []byte(`{}`))
		q1.Enqueue(ctx, env)
		q1.Dequeue(ctx) // Mark as processing
		q1.Close()      // Crash without completing

		// Second instance: should reset to pending
		q2, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q2.Close()

		// Should be able to dequeue the "stuck" job again
		job, err := q2.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue recovered job: %v", err)
		}
		if job.ID != env.ID {
			t.Error("stuck job not recovered")
		}
	})
}

func TestSQLiteQueue_Cleanup(t *testing.T) {
	t.Run("closes database connection", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		if err := q.Close(); err != nil {
			t.Errorf("Close failed: %v", err)
		}

		// Verify database file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Error("database file should exist after close")
		}
	})
}

func TestSQLiteQueue_ListJobs(t *testing.T) {
	t.Run("lists jobs by status", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		// Create jobs with different statuses
		pending1, _ := jobs.NewEnvelope("pending1", []byte(`{}`))
		pending2, _ := jobs.NewEnvelope("pending2", []byte(`{}`))
		q.Enqueue(ctx, pending1)
		q.Enqueue(ctx, pending2)

		processing, _ := jobs.NewEnvelope("processing", []byte(`{}`))
		q.Enqueue(ctx, processing)
		q.Dequeue(ctx) // Mark as processing

		completed, _ := jobs.NewEnvelope("completed", []byte(`{}`))
		q.Enqueue(ctx, completed)
		job, _ := q.Dequeue(ctx)
		q.CompleteJob(ctx, job.ID)

		// List pending jobs
		pendingJobs := q.ListJobsByStatus(ctx, "pending")
		if len(pendingJobs) != 2 {
			t.Errorf("got %d pending jobs, want 2", len(pendingJobs))
		}

		// List processing jobs
		processingJobs := q.ListJobsByStatus(ctx, "processing")
		if len(processingJobs) != 1 {
			t.Errorf("got %d processing jobs, want 1", len(processingJobs))
		}

		// List completed jobs
		completedJobs := q.ListJobsByStatus(ctx, "completed")
		if len(completedJobs) != 1 {
			t.Errorf("got %d completed jobs, want 1", len(completedJobs))
		}
	})
}

func TestSQLiteQueue_RetryState(t *testing.T) {
	t.Run("increments attempt count", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("retry", []byte(`{}`))
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		if job.Attempts != 0 {
			t.Errorf("initial attempts: got %d, want 0", job.Attempts)
		}

		// Increment and re-enqueue for retry
		job.Attempts++
		q.RequeueJob(ctx, job)

		retriedJob, _ := q.Dequeue(ctx)
		if retriedJob.Attempts != 1 {
			t.Errorf("after retry: got %d attempts, want 1", retriedJob.Attempts)
		}
	})

	t.Run("respects max retries", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		env, _ := jobs.NewEnvelope("limited", []byte(`{}`))
		env.MaxRetries = 3
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		if job.MaxRetries != 3 {
			t.Errorf("got max_retries %d, want 3", job.MaxRetries)
		}
	})
}
