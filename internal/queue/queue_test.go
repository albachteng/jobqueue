package queue

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueDequeue(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[string]()

	job := "hello world"
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if got != job {
		t.Errorf("got %q, want %q", got, job)
	}
}

func TestDequeueEmpty(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[string]()

	_, err := q.Dequeue(ctx)
	if err == nil {
		t.Error("expected error when dequeuing from empty queue, got nil")
	}
}

func TestEnqueueMultiple(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[string]()

	jobs := []string{"job1", "job2", "job3"}
	for _, job := range jobs {
		if err := q.Enqueue(ctx, job); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	for _, want := range jobs {
		got, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestFIFOOrder(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[int]()

	for i := 1; i <= 5; i++ {
		if err := q.Enqueue(ctx, i); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	for i := 1; i <= 5; i++ {
		got, err := q.Dequeue(ctx)
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}
		if got != i {
			t.Errorf("got %d, want %d (FIFO order broken)", got, i)
		}
	}
}

func TestContextCancellation(t *testing.T) {
	q := NewInMemoryQueue[string]()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := q.Enqueue(ctx, "test")
	if err == nil {
		t.Error("expected error when enqueuing with cancelled context, got nil")
	}
}

func TestContextTimeout(t *testing.T) {
	q := NewInMemoryQueue[string]()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	time.Sleep(10 * time.Millisecond)

	err := q.Enqueue(ctx, "test")
	if err == nil {
		t.Error("expected error when enqueuing with expired context, got nil")
	}
}

func TestConcurrentEnqueue(t *testing.T) {
	ctx := context.Background()
	q := NewInMemoryQueue[int]()

	numGoroutines := 10
	jobsPerGoroutine := 100

	done := make(chan bool)
	for i := 0; i < numGoroutines; i++ {
		go func(offset int) {
			for j := 0; j < jobsPerGoroutine; j++ {
				if err := q.Enqueue(ctx, offset*jobsPerGoroutine+j); err != nil {
					t.Errorf("Enqueue failed: %v", err)
				}
			}
			done <- true
		}(i)
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	count := 0
	for {
		_, err := q.Dequeue(ctx)
		if err != nil {
			break
		}
		count++
	}

	expected := numGoroutines * jobsPerGoroutine
	if count != expected {
		t.Errorf("got %d jobs, want %d", count, expected)
	}
}
