package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

// waitForCondition polls a condition function until it returns true or times out
func waitForCondition(t *testing.T, condition func() bool, timeout time.Duration, message string) {
	t.Helper()
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting for: %s", message)
		case <-ticker.C:
			if condition() {
				return
			}
		}
	}
}

func TestDispatcher_StartsWorkers(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	for i := 0; i < 5; i++ {
		envelope := &jobs.Envelope{
			ID:     jobs.JobID("job-" + string(rune('a'+i))),
			Type:   "test",
			Status: "pending",
		}
		q.Enqueue(ctx, envelope)
	}

	var processed []*jobs.Envelope
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		processed = append(processed, env)
		mu.Unlock()
		return nil
	})

	registry.Register("test", handler)

	dispatcher := NewDispatcher(q, 2, registry, nil, nil)

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == 5
	}, 2*time.Second, "all 5 jobs to be processed")

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(processed) != 5 {
		t.Errorf("processed %d jobs, want 5", len(processed))
	}
}

func TestDispatcher_RoutesEnvelopes(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	for i := 0; i < 3; i++ {
		q.Enqueue(ctx, &jobs.Envelope{
			ID:     jobs.JobID("echo-" + string(rune('a'+i))),
			Type:   "echo",
			Status: "pending",
		})
	}
	for i := 0; i < 2; i++ {
		q.Enqueue(ctx, &jobs.Envelope{
			ID:     jobs.JobID("email-" + string(rune('a'+i))),
			Type:   "email",
			Status: "pending",
		})
	}

	var echoCount, emailCount int
	var mu sync.Mutex

	echoHandler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		echoCount++
		mu.Unlock()
		return nil
	})

	emailHandler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		emailCount++
		mu.Unlock()
		return nil
	})

	registry.Register("echo", echoHandler)
	registry.Register("email", emailHandler)

	dispatcher := NewDispatcher(q, 2, registry, nil, nil)

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return echoCount == 3 && emailCount == 2
	}, 2*time.Second, "all jobs to be routed and processed")

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if echoCount != 3 {
		t.Errorf("processed %d echo jobs, want 3", echoCount)
	}
	if emailCount != 2 {
		t.Errorf("processed %d email jobs, want 2", emailCount)
	}
}

func TestDispatcher_ConcurrentProcessing(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	numJobs := 10
	for i := 0; i < numJobs; i++ {
		q.Enqueue(ctx, &jobs.Envelope{
			ID:     jobs.JobID("concurrent-" + string(rune('a'+i))),
			Type:   "concurrent",
			Status: "pending",
		})
	}

	var count int
	var mu sync.Mutex
	var maxConcurrent int
	var currentConcurrent int

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		currentConcurrent++
		if currentConcurrent > maxConcurrent {
			maxConcurrent = currentConcurrent
		}
		mu.Unlock()

		time.Sleep(20 * time.Millisecond)

		mu.Lock()
		currentConcurrent--
		count++
		mu.Unlock()

		return nil
	})

	registry.Register("concurrent", handler)

	numWorkers := 3
	dispatcher := NewDispatcher(q, numWorkers, registry, nil, nil)

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == numJobs
	}, 2*time.Second, "all 10 jobs to be processed")

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if count != numJobs {
		t.Errorf("processed %d jobs, want %d", count, numJobs)
	}

	if maxConcurrent < 2 {
		t.Errorf("max concurrent workers was %d, expected at least 2 for worker pool of %d", maxConcurrent, numWorkers)
	}
}

func TestDispatcher_GracefulShutdown(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	for i := 0; i < 5; i++ {
		q.Enqueue(ctx, &jobs.Envelope{
			ID:     jobs.JobID("shutdown-" + string(rune('a'+i))),
			Type:   "shutdown",
			Status: "pending",
		})
	}

	var completed []jobs.JobID
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		completed = append(completed, env.ID)
		mu.Unlock()

		return nil
	})

	registry.Register("shutdown", handler)

	dispatcher := NewDispatcher(q, 2, registry, nil, nil)

	dispatchCtx, cancel := context.WithCancel(ctx)

	go dispatcher.Start(dispatchCtx)

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(completed) > 0
	}, 2*time.Second, "at least one job to start processing")

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(completed) == 0 {
		t.Error("no jobs completed, graceful shutdown may not be working")
	}

	t.Logf("Completed %d jobs during graceful shutdown", len(completed))
}

func TestDispatcher_HandlesEmptyQueue(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	var count int
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})

	registry.Register("delayed", handler)

	dispatcher := NewDispatcher(q, 2, registry, nil, nil)

	dispatchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	time.Sleep(20 * time.Millisecond)
	q.Enqueue(ctx, &jobs.Envelope{
		ID:     "delayed-1",
		Type:   "delayed",
		Status: "pending",
	})

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == 1
	}, 2*time.Second, "delayed job to be processed")

	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if count != 1 {
		t.Errorf("processed %d jobs, want 1", count)
	}
}

func TestDispatcher_StopWithoutStart(t *testing.T) {
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	})

	registry.Register("noop", handler)

	dispatcher := NewDispatcher(q, 2, registry, nil, nil)

	dispatcher.Stop()
}

func TestDispatcher_WorkersShareRegistry(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	for i := 0; i < 10; i++ {
		q.Enqueue(ctx, &jobs.Envelope{
			ID:     jobs.JobID("shared-" + string(rune('a'+i))),
			Type:   "shared",
			Status: "pending",
		})
	}

	var count int
	var mu sync.Mutex
	uniqueWorkers := make(map[int]bool)

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	registry.Register("shared", handler)

	numWorkers := 3
	dispatcher := NewDispatcher(q, numWorkers, registry, nil, nil)

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return count == 10
	}, 2*time.Second, "all 10 jobs to be processed")

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if count != 10 {
		t.Errorf("processed %d jobs, want 10", count)
	}

	_ = uniqueWorkers
}

func TestDispatcher_NoJobProcessedTwice(t *testing.T) {
	ctx := context.Background()
	q := queue.NewInMemoryQueue[*jobs.Envelope]()
	registry := jobs.NewRegistry()

	numJobs := 20
	for i := 0; i < numJobs; i++ {
		q.Enqueue(ctx, &jobs.Envelope{
			ID:     jobs.JobID("unique-" + string(rune('a'+i%26)) + string(rune('a'+i/26))),
			Type:   "unique",
			Status: "pending",
		})
	}

	processedIDs := make(map[jobs.JobID]int)
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		processedIDs[env.ID]++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return nil
	})

	registry.Register("unique", handler)

	numWorkers := 4
	dispatcher := NewDispatcher(q, numWorkers, registry, nil, nil)

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	waitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processedIDs) == numJobs
	}, 2*time.Second, "all 20 jobs to be processed")

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	for id, count := range processedIDs {
		if count != 1 {
			t.Errorf("job %s processed %d times, want 1", id, count)
		}
	}

	if len(processedIDs) != numJobs {
		t.Errorf("processed %d unique jobs, want %d", len(processedIDs), numJobs)
	}
}
