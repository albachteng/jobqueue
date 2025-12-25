package queue

import "context"

/*
DO NOT: type Worker[T any] struct { ... }
execute behavior, not types
workers often deal with side-effects
best expressed with interfaces
generics can go inside the envelope, as below:
*/

// HandlerFunc processes a job - for now using map[string]string
// Later we'll move to Handler interface with JobEnvelope
type HandlerFunc func(context.Context, map[string]string) error

// Worker processes jobs from a channel
type Worker struct {
	handler HandlerFunc
}

// NewWorker creates a new worker with the given handler
func NewWorker(handler HandlerFunc) *Worker {
	return &Worker{
		handler: handler,
	}
}

// Start begins processing jobs from the channel until it's closed or context is cancelled
func (w *Worker) Start(ctx context.Context, jobs <-chan map[string]string) {
	// TODO: implement
}
