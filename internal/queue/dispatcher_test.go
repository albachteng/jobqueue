package queue

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDispatcherStartsWorkers(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[map[string]string]()

	for i := 0; i < 5; i++ {
		q.Enqueue(ctx, map[string]string{"id": string(rune(i))})
	}

	var processed []map[string]string
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		mu.Lock()
		processed = append(processed, job)
		mu.Unlock()
		return nil
	}

	dispatcher := NewDispatcher(q, 2, handler) 

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	time.Sleep(100 * time.Millisecond)
	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(processed) != 5 {
		t.Errorf("processed %d jobs, want 5", len(processed))
	}
}

func TestDispatcherConcurrentProcessing(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[map[string]string]()

	numJobs := 10
	for i := 0; i < numJobs; i++ {
		q.Enqueue(ctx, map[string]string{"id": string(rune(i))})
	}

	var count int
	var mu sync.Mutex
	var maxConcurrent int
	var currentConcurrent int

	handler := func(ctx context.Context, job map[string]string) error {
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
	}

	numWorkers := 3
	dispatcher := NewDispatcher(q, numWorkers, handler)

	dispatchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	time.Sleep(200 * time.Millisecond)
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

func TestDispatcherGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[map[string]string]()

	for i := 0; i < 5; i++ {
		q.Enqueue(ctx, map[string]string{"id": string(rune(i))})
	}

	var completed []string
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		completed = append(completed, job["id"])
		mu.Unlock()

		return nil
	}

	dispatcher := NewDispatcher(q, 2, handler)

	dispatchCtx, cancel := context.WithCancel(ctx)

	go dispatcher.Start(dispatchCtx)

	time.Sleep(30 * time.Millisecond)

	cancel()
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if len(completed) == 0 {
		t.Error("no jobs completed, graceful shutdown may not be working")
	}

	t.Logf("Completed %d jobs during graceful shutdown", len(completed))
}

func TestDispatcherHandlesEmptyQueue(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[map[string]string]()

	var count int
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}

	dispatcher := NewDispatcher(q, 2, handler)

	dispatchCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	go dispatcher.Start(dispatchCtx)

	time.Sleep(20 * time.Millisecond)
	q.Enqueue(ctx, map[string]string{"message": "delayed job"})

	time.Sleep(50 * time.Millisecond)
	dispatcher.Stop()

	mu.Lock()
	defer mu.Unlock()

	if count != 1 {
		t.Errorf("processed %d jobs, want 1", count)
	}
}

func TestDispatcherStopWithoutStart(t *testing.T) {
	q := NewInMemoryQueue[map[string]string]()
	handler := func(ctx context.Context, job map[string]string) error {
		return nil
	}

	dispatcher := NewDispatcher(q, 2, handler)

	dispatcher.Stop()
}
