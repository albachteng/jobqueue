package queue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWorkerProcessesJob(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan map[string]string, 1)

	var processed map[string]string
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		mu.Lock()
		processed = job
		mu.Unlock()
		return nil
	}

	worker := NewWorker(handler)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	job := map[string]string{"message": "test job"}
	jobChan <- job
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if processed == nil {
		t.Fatal("job was not processed")
	}
	if processed["message"] != "test job" {
		t.Errorf("got message %q, want %q", processed["message"], "test job")
	}
}

func TestWorkerHandlesMultipleJobs(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan map[string]string, 10)

	var count int
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	}

	worker := NewWorker(handler)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	for i := 0; i < 5; i++ {
		jobChan <- map[string]string{"id": string(rune(i))}
	}
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if count != 5 {
		t.Errorf("processed %d jobs, want 5", count)
	}
}

func TestWorkerRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobChan := make(chan map[string]string, 10)

	var count int
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		mu.Lock()
		count++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	worker := NewWorker(handler)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	for i := 0; i < 10; i++ {
		jobChan <- map[string]string{"id": string(rune(i))}
	}

	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Worker stopped
	case <-time.After(100 * time.Millisecond):
		t.Fatal("worker did not stop after context cancellation")
	}

	mu.Lock()
	defer mu.Unlock()

	if count >= 10 {
		t.Errorf("processed %d jobs after cancellation, expected fewer", count)
	}
}

func TestWorkerHandlesErrors(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan map[string]string, 5)

	var count int
	var mu sync.Mutex

	handler := func(ctx context.Context, job map[string]string) error {
		mu.Lock()
		count++
		mu.Unlock()

		if count%2 == 0 {
			return errors.New("simulated error")
		}
		return nil
	}

	worker := NewWorker(handler)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	for i := 0; i < 5; i++ {
		jobChan <- map[string]string{"id": string(rune(i))}
	}
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if count != 5 {
		t.Errorf("processed %d jobs, want 5 (should continue after errors)", count)
	}
}
