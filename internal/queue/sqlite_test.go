package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
)

func TestSQLiteQueue_Migration(t *testing.T) {
	t.Run("migrates old database without priority column", func(t *testing.T) {
		dbPath := t.TempDir() + "/old.db"

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		}

		oldSchema := `
		CREATE TABLE jobs (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			payload BLOB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			last_error TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			processed_at TIMESTAMP
		);
		`

		if _, err := db.Exec(oldSchema); err != nil {
			t.Fatal("Failed to create old schema:", err)
		}

		now := time.Now()
		_, err = db.Exec(`
			INSERT INTO jobs (id, type, payload, status, attempts, max_retries, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, "test-job-1", "test", []byte(`{}`), "pending", 0, 3, now, now)

		if err != nil {
			t.Fatal("Failed to insert job:", err)
		}

		db.Close()

		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database with migrations: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Failed to dequeue after migration: %v", err)
		}

		if job.ID != "test-job-1" {
			t.Errorf("expected job ID 'test-job-1', got %s", job.ID)
		}

		if job.Priority != 0 {
			t.Errorf("expected priority 0 (default), got %d", job.Priority)
		}
	})

	t.Run("migrates old database without scheduled_at column", func(t *testing.T) {
		dbPath := t.TempDir() + "/old_no_scheduled.db"

		db, err := sql.Open("sqlite3", dbPath)
		if err != nil {
			t.Fatal(err)
		}

		oldSchema := `
		CREATE TABLE jobs (
			id TEXT PRIMARY KEY,
			type TEXT NOT NULL,
			payload BLOB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			priority INTEGER NOT NULL DEFAULT 0,
			attempts INTEGER NOT NULL DEFAULT 0,
			max_retries INTEGER NOT NULL DEFAULT 3,
			last_error TEXT,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP NOT NULL,
			processed_at TIMESTAMP
		);
		`

		if _, err := db.Exec(oldSchema); err != nil {
			t.Fatal("Failed to create old schema:", err)
		}

		now := time.Now()
		_, err = db.Exec(`
			INSERT INTO jobs (id, type, payload, status, priority, attempts, max_retries, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, "test-job-2", "test", []byte(`{}`), "pending", 0, 0, 3, now, now)

		if err != nil {
			t.Fatal("Failed to insert job:", err)
		}

		db.Close()

		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("Failed to open database with migrations: %v", err)
		}
		defer q.Close()

		ctx := context.Background()
		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Failed to dequeue after migration: %v", err)
		}

		if job.ID != "test-job-2" {
			t.Errorf("expected job ID 'test-job-2', got %s", job.ID)
		}

		if job.ScheduledAt != nil {
			t.Errorf("expected nil ScheduledAt after migration, got %v", job.ScheduledAt)
		}
	})
}

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

		numJobs := 20 // Avoid SQLite write lock contention with high concurrency
		for i := 0; i < numJobs; i++ {
			go func() {
				env, _ := jobs.NewEnvelope("concurrent", []byte(`{}`))
				q.Enqueue(ctx, env)
				done <- struct{}{}
			}()
		}

		for i := 0; i < numJobs; i++ {
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

		if count != numJobs {
			t.Errorf("got %d jobs, want %d", count, numJobs)
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

		lowPriority, _ := jobs.NewEnvelope("low", []byte(`{}`))
		lowPriority.Priority = 1

		mediumPriority, _ := jobs.NewEnvelope("medium", []byte(`{}`))
		mediumPriority.Priority = 5

		highPriority, _ := jobs.NewEnvelope("high", []byte(`{}`))
		highPriority.Priority = 10

		q.Enqueue(ctx, lowPriority)
		q.Enqueue(ctx, mediumPriority)
		q.Enqueue(ctx, highPriority)

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

		job1, _ := jobs.NewEnvelope("first", []byte(`{}`))
		job1.Priority = 5

		job2, _ := jobs.NewEnvelope("second", []byte(`{}`))
		job2.Priority = 5

		job3, _ := jobs.NewEnvelope("third", []byte(`{}`))
		job3.Priority = 5

		q.Enqueue(ctx, job1)
		q.Enqueue(ctx, job2)
		q.Enqueue(ctx, job3)

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

		defaultJob, _ := jobs.NewEnvelope("default", []byte(`{}`))
		// Don't set priority - should default to 0

		highJob, _ := jobs.NewEnvelope("high", []byte(`{}`))
		highJob.Priority = 10

		q.Enqueue(ctx, defaultJob)
		q.Enqueue(ctx, highJob)

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

		q2, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to reopen queue: %v", err)
		}
		defer q2.Close()

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

func TestSQLiteQueue_DeadLetterQueue(t *testing.T) {
	t.Run("moves failed job to DLQ with error", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		env, _ := jobs.NewEnvelope("test", []byte(`{}`))
		env.MaxRetries = 3
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		job.Attempts = 4 // Exceeded max retries

		errorMsg := "permanent failure after 4 attempts"

		err = q.MoveToDLQ(ctx, job, errorMsg)
		if err != nil {
			t.Fatalf("MoveToDLQ failed: %v", err)
		}

		record, exists := q.GetJob(ctx, job.ID)
		if !exists {
			t.Fatal("job not found after moving to DLQ")
		}

		if record.Status != "dlq" {
			t.Errorf("expected status 'dlq', got %s", record.Status)
		}

		if record.LastError != errorMsg {
			t.Errorf("expected error %q, got %q", errorMsg, record.LastError)
		}

		if record.Attempts != 4 {
			t.Errorf("expected 4 attempts, got %d", record.Attempts)
		}
	})

	t.Run("lists all DLQ jobs", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		jobIDs := make([]jobs.JobID, 3)
		for i := 0; i < 3; i++ {
			env, _ := jobs.NewEnvelope(jobs.JobType("dlq-test"), []byte(`{}`))
			q.Enqueue(ctx, env)
			job, _ := q.Dequeue(ctx)
			q.MoveToDLQ(ctx, job, "test error")
			jobIDs[i] = job.ID
		}

		normalEnv, _ := jobs.NewEnvelope("normal", []byte(`{}`))
		q.Enqueue(ctx, normalEnv)

		dlqJobs := q.ListDLQJobs(ctx)

		if len(dlqJobs) != 3 {
			t.Errorf("expected 3 DLQ jobs, got %d", len(dlqJobs))
		}

		for _, record := range dlqJobs {
			if record.Status != "dlq" {
				t.Errorf("expected status 'dlq', got %s", record.Status)
			}
		}
	})

	t.Run("requeues job from DLQ", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		env, _ := jobs.NewEnvelope("requeue-test", []byte(`{}`))
		env.MaxRetries = 3
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		job.Attempts = 4
		q.MoveToDLQ(ctx, job, "original error")

		err = q.RequeueDLQJob(ctx, job.ID)
		if err != nil {
			t.Fatalf("RequeueDLQJob failed: %v", err)
		}

		record, exists := q.GetJob(ctx, job.ID)
		if !exists {
			t.Fatal("job not found after requeue")
		}

		if record.Status != "pending" {
			t.Errorf("expected status 'pending', got %s", record.Status)
		}

		if record.Attempts != 0 {
			t.Errorf("expected 0 attempts after requeue, got %d", record.Attempts)
		}

		requeuedJob, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue requeued job: %v", err)
		}

		if requeuedJob.ID != job.ID {
			t.Errorf("dequeued wrong job: got %s, want %s", requeuedJob.ID, job.ID)
		}
	})

	t.Run("DLQ job preserves payload and metadata", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		payload := json.RawMessage(`{"message":"test data"}`)
		env, _ := jobs.NewEnvelope("preserve-test", payload)
		env.Priority = 10
		env.MaxRetries = 5
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		job.Attempts = 6

		q.MoveToDLQ(ctx, job, "max retries exceeded")

		record, exists := q.GetJob(ctx, job.ID)
		if !exists {
			t.Fatal("job not found in DLQ")
		}

		if string(record.Payload) != string(payload) {
			t.Errorf("payload not preserved: got %s, want %s", record.Payload, payload)
		}

		if record.Priority != 10 {
			t.Errorf("priority not preserved: got %d, want 10", record.Priority)
		}

		if record.MaxRetries != 5 {
			t.Errorf("max_retries not preserved: got %d, want 5", record.MaxRetries)
		}

		if record.Type != "preserve-test" {
			t.Errorf("type not preserved: got %s, want preserve-test", record.Type)
		}
	})

	t.Run("cannot requeue job not in DLQ", func(t *testing.T) {
		dbPath := filepath.Join(t.TempDir(), "test.db")
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer q.Close()

		ctx := context.Background()

		env, _ := jobs.NewEnvelope("pending-job", []byte(`{}`))
		q.Enqueue(ctx, env)

		job, _ := q.Dequeue(ctx)
		q.CompleteJob(ctx, job.ID)

		err = q.RequeueDLQJob(ctx, job.ID)
		if err == nil {
			t.Error("expected error when requeueing non-DLQ job, got nil")
		}

		expectedErr := "job is not in DLQ"
		if err.Error() != expectedErr {
			t.Errorf("expected error %q, got %q", expectedErr, err.Error())
		}
	})
}

func TestSQLiteQueue_ScheduledJobs(t *testing.T) {
	t.Run("does not dequeue jobs scheduled in the future", func(t *testing.T) {
		dbPath := t.TempDir() + "/scheduled.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		futureTime := time.Now().Add(1 * time.Hour)
		env, _ := jobs.NewEnvelope("future-job", []byte(`{}`))
		env.ScheduledAt = &futureTime

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue scheduled job: %v", err)
		}

		job, err := q.Dequeue(ctx)
		if err != ErrEmptyQueue {
			t.Errorf("expected ErrEmptyQueue, got: %v, job: %v", err, job)
		}
	})

	t.Run("dequeues jobs scheduled in the past", func(t *testing.T) {
		dbPath := t.TempDir() + "/past.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		pastTime := time.Now().Add(-1 * time.Hour)
		env, _ := jobs.NewEnvelope("past-job", []byte(`{}`))
		env.ScheduledAt = &pastTime

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue past scheduled job: %v", err)
		}

		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue past scheduled job: %v", err)
		}

		if job.ID != env.ID {
			t.Errorf("expected job ID %s, got %s", env.ID, job.ID)
		}
	})

	t.Run("dequeues jobs scheduled for now", func(t *testing.T) {
		dbPath := t.TempDir() + "/now.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		now := time.Now()
		env, _ := jobs.NewEnvelope("now-job", []byte(`{}`))
		env.ScheduledAt = &now

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue job: %v", err)
		}

		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue job: %v", err)
		}

		if job.ID != env.ID {
			t.Errorf("expected job ID %s, got %s", env.ID, job.ID)
		}
	})

	t.Run("immediate jobs (nil ScheduledAt) are dequeued first", func(t *testing.T) {
		dbPath := t.TempDir() + "/immediate.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		futureTime := time.Now().Add(-10 * time.Minute)
		scheduledEnv, _ := jobs.NewEnvelope("scheduled", []byte(`{}`))
		scheduledEnv.ScheduledAt = &futureTime
		q.Enqueue(ctx, scheduledEnv)

		immediateEnv, _ := jobs.NewEnvelope("immediate", []byte(`{}`))
		q.Enqueue(ctx, immediateEnv)

		job, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("failed to dequeue: %v", err)
		}

		if job.Type != "immediate" {
			t.Errorf("expected immediate job first, got type: %s", job.Type)
		}
	})

	t.Run("scheduled jobs ordered by scheduled time then priority", func(t *testing.T) {
		dbPath := t.TempDir() + "/ordering.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()

		ctx := context.Background()

		now := time.Now()

		// Enqueue jobs with different scheduled times and priorities
		// Job 1: scheduled 2 min ago, priority 0
		time1 := now.Add(-2 * time.Minute)
		env1, _ := jobs.NewEnvelope("job1", []byte(`{}`))
		env1.ScheduledAt = &time1
		env1.Priority = 0
		q.Enqueue(ctx, env1)

		// Job 2: scheduled 1 min ago, priority 10 (higher)
		time2 := now.Add(-1 * time.Minute)
		env2, _ := jobs.NewEnvelope("job2", []byte(`{}`))
		env2.ScheduledAt = &time2
		env2.Priority = 10
		q.Enqueue(ctx, env2)

		// Job 3: scheduled 1 min ago, priority 5
		time3 := now.Add(-1 * time.Minute)
		env3, _ := jobs.NewEnvelope("job3", []byte(`{}`))
		env3.ScheduledAt = &time3
		env3.Priority = 5
		q.Enqueue(ctx, env3)

		// Dequeue order should be: job1 (earliest), then job2 (higher priority), then job3
		job1, _ := q.Dequeue(ctx)
		if job1.Type != "job1" {
			t.Errorf("expected job1 first (earliest scheduled), got: %s", job1.Type)
		}

		job2, _ := q.Dequeue(ctx)
		if job2.Type != "job2" {
			t.Errorf("expected job2 second (higher priority), got: %s", job2.Type)
		}

		job3, _ := q.Dequeue(ctx)
		if job3.Type != "job3" {
			t.Errorf("expected job3 third, got: %s", job3.Type)
		}
	})

	t.Run("preserves scheduled time across persistence", func(t *testing.T) {
		dbPath := t.TempDir() + "/persist.db"
		q, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.Background()

		scheduledTime := time.Now().Add(1 * time.Hour)
		env, _ := jobs.NewEnvelope("scheduled", []byte(`{}`))
		env.ScheduledAt = &scheduledTime

		if err := q.Enqueue(ctx, env); err != nil {
			t.Fatalf("failed to enqueue: %v", err)
		}

		q.Close()

		q2, err := NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		defer q2.Close()

		record, exists := q2.GetJob(ctx, env.ID)
		if !exists {
			t.Fatal("job not found after reopening database")
		}

		if record.ScheduledAt == nil {
			t.Error("scheduled time was lost")
		} else if !record.ScheduledAt.Equal(scheduledTime) {
			t.Errorf("scheduled time mismatch: got %v, want %v", record.ScheduledAt, scheduledTime)
		}
	})
}
