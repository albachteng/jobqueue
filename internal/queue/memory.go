package queue

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrEmptyQueue        = errors.New("queue is empty")
	ErrJobNotFound       = errors.New("job not found")
	ErrJobNotCancellable = errors.New("job cannot be cancelled in current state")
)

type InMemoryQueue[T any] struct {
	mu    sync.Mutex
	items []T
}

func NewInMemoryQueue[T any]() *InMemoryQueue[T] {
	return &InMemoryQueue[T]{
		items: make([]T, 0),
	}
}

func (q *InMemoryQueue[T]) Enqueue(ctx context.Context, job T) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	q.items = append(q.items, job)
	return nil
}

func (q *InMemoryQueue[T]) Dequeue(ctx context.Context) (T, error) {
	var zero T

	if err := ctx.Err(); err != nil {
		return zero, err
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return zero, ErrEmptyQueue
	}

	job := q.items[0]
	q.items = q.items[1:]
	return job, nil
}
