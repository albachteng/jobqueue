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

		for i := 0; i < 100; i++ {
			go func() {
				env, _ := jobs.NewEnvelope("concurrent", []byte(`{}`))
				q.Enqueue(ctx, env)
				done <- struct{}{}
			}()
		}

		for i := 0; i < 100; i++ {
			<-done
		}

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

		_, err = q.Dequeue(ctx)
		if err != ErrEmptyQueue {
			t.Errorf("dequeued job should not be available again, got job instead of ErrEmptyQueue")
		}

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

		pendingJobs := q.ListJobsByStatus(ctx, "pending")
		if len(pendingJobs) != 2 {
			t.Errorf("got %d pending jobs, want 2", len(pendingJobs))
		}

		processingJobs := q.ListJobsByStatus(ctx, "processing")
		if len(processingJobs) != 1 {
			t.Errorf("got %d processing jobs, want 1", len(processingJobs))
		}

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

func TestSQLiteQueue_PriorityQueuing(t *testing.T) {
	t.Run("dequeues higher priority jobs first", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		// Enqueue jobs with different priorities
		lowPriority, _ := jobs.NewEnvelope("low", []byte(`{}`))
		lowPriority.Priority = 1

		mediumPriority, _ := jobs.NewEnvelope("medium", []byte(`{}`))
		mediumPriority.Priority = 5

		highPriority, _ := jobs.NewEnvelope("high", []byte(`{}`))
		highPriority.Priority = 10

		// Enqueue in low-to-high order
		q.Enqueue(ctx, lowPriority)
		q.Enqueue(ctx, mediumPriority)
		q.Enqueue(ctx, highPriority)

		// Should dequeue in high-to-low priority order
		job1, _ := q.Dequeue(ctx)
		if job1.Priority != 10 {
			t.Errorf("first job: got priority %d, want 10", job1.Priority)
		}

		job2, _ := q.Dequeue(ctx)
		if job2.Priority != 5 {
			t.Errorf("second job: got priority %d, want 5", job2.Priority)
		}

		job3, _ := q.Dequeue(ctx)
		if job3.Priority != 1 {
			t.Errorf("third job: got priority %d, want 1", job3.Priority)
		}
	})

	t.Run("maintains FIFO order within same priority", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		// Enqueue three jobs with same priority
		job1, _ := jobs.NewEnvelope("first", []byte(`{}`))
		job1.Priority = 5

		job2, _ := jobs.NewEnvelope("second", []byte(`{}`))
		job2.Priority = 5

		job3, _ := jobs.NewEnvelope("third", []byte(`{}`))
		job3.Priority = 5

		q.Enqueue(ctx, job1)
		q.Enqueue(ctx, job2)
		q.Enqueue(ctx, job3)

		// Should dequeue in insertion order
		dequeued1, _ := q.Dequeue(ctx)
		if dequeued1.Type != "first" {
			t.Errorf("first dequeued: got type %s, want 'first'", dequeued1.Type)
		}

		dequeued2, _ := q.Dequeue(ctx)
		if dequeued2.Type != "second" {
			t.Errorf("second dequeued: got type %s, want 'second'", dequeued2.Type)
		}

		dequeued3, _ := q.Dequeue(ctx)
		if dequeued3.Type != "third" {
			t.Errorf("third dequeued: got type %s, want 'third'", dequeued3.Type)
		}
	})

	t.Run("handles default priority correctly", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		// Enqueue job without setting priority (should default to 0)
		defaultJob, _ := jobs.NewEnvelope("default", []byte(`{}`))
		// Don't set priority - should default to 0

		highJob, _ := jobs.NewEnvelope("high", []byte(`{}`))
		highJob.Priority = 10

		q.Enqueue(ctx, defaultJob)
		q.Enqueue(ctx, highJob)

		// High priority should come first
		job1, _ := q.Dequeue(ctx)
		if job1.Type != "high" {
			t.Errorf("first job: got type %s, want 'high'", job1.Type)
		}

		job2, _ := q.Dequeue(ctx)
		if job2.Type != "default" {
			t.Errorf("second job: got type %s, want 'default'", job2.Type)
		}
	})

	t.Run("handles negative priorities", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		negativeJob, _ := jobs.NewEnvelope("negative", []byte(`{}`))
		negativeJob.Priority = -5

		defaultJob, _ := jobs.NewEnvelope("default", []byte(`{}`))
		defaultJob.Priority = 0

		positiveJob, _ := jobs.NewEnvelope("positive", []byte(`{}`))
		positiveJob.Priority = 5

		q.Enqueue(ctx, negativeJob)
		q.Enqueue(ctx, defaultJob)
		q.Enqueue(ctx, positiveJob)

		// Should dequeue: positive, default, negative
		job1, _ := q.Dequeue(ctx)
		if job1.Type != "positive" {
			t.Errorf("first job: got type %s, want 'positive'", job1.Type)
		}

		job2, _ := q.Dequeue(ctx)
		if job2.Type != "default" {
			t.Errorf("second job: got type %s, want 'default'", job2.Type)
		}

		job3, _ := q.Dequeue(ctx)
		if job3.Type != "negative" {
			t.Errorf("third job: got type %s, want 'negative'", job3.Type)
		}
	})

	t.Run("persists priority across restarts", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")

		q1, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}

		ctx := context.Background()

		lowJob, _ := jobs.NewEnvelope("low", []byte(`{}`))
		lowJob.Priority = 1

		highJob, _ := jobs.NewEnvelope("high", []byte(`{}`))
		highJob.Priority = 10

		q1.Enqueue(ctx, lowJob)
		q1.Enqueue(ctx, highJob)
		q1.Close()

		// Reopen database
		q2, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q2.Close()

		// High priority should still come first after restart
		job1, _ := q2.Dequeue(ctx)
		if job1.Priority != 10 {
			t.Errorf("first job after restart: got priority %d, want 10", job1.Priority)
		}
		if job1.Type != "high" {
			t.Errorf("first job after restart: got type %s, want 'high'", job1.Type)
		}

		job2, _ := q2.Dequeue(ctx)
		if job2.Priority != 1 {
			t.Errorf("second job after restart: got priority %d, want 1", job2.Priority)
		}
	})

	t.Run("complex priority scenario", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		// Create a mix of priorities enqueued in random order
		testJobs := []struct {
			typ      jobs.JobType
			priority int
		}{
			{"job1", 5},
			{"job2", 10},
			{"job3", 1},
			{"job4", 10}, // Same priority as job2
			{"job5", 0},
			{"job6", 5}, // Same priority as job1
		}

		for _, j := range testJobs {
			env, _ := jobs.NewEnvelope(j.typ, []byte(`{}`))
			env.Priority = j.priority
			q.Enqueue(ctx, env)
		}

		// Expected dequeue order (higher priority first):
		// job2 (10), job4 (10) - FIFO within priority 10
		// job1 (5), job6 (5) - FIFO within priority 5
		// job3 (1)
		// job5 (0)
		expectedOrder := []jobs.JobType{"job2", "job4", "job1", "job6", "job3", "job5"}

		for i, expectedType := range expectedOrder {
			job, err := q.Dequeue(ctx)
			if err != nil {
				t.Fatalf("dequeue %d failed: %v", i, err)
			}
			if job.Type != expectedType {
				t.Errorf("position %d: got type %s, want %s", i, job.Type, expectedType)
			}
		}
	})
}
