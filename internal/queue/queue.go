package queue

import "context"

type Queue[T any] interface {
	Enqueue(ctx context.Context, job T) error
	Dequeue(ctx context.Context) (T, error)
}
