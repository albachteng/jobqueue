package queue

import (
	"context"
	"log"
)

/*
DO NOT: type Worker[T any] struct { ... }
execute behavior, not types
workers often deal with side-effects
best expressed with interfaces
generics can go inside the envelope
*/

// HandlerFunc processes a job - for now using map[string]string
// Later we'll move to Handler interface with JobEnvelope
type HandlerFunc func(context.Context, map[string]string) error

type Worker struct {
	handler HandlerFunc
}

func NewWorker(handler HandlerFunc) *Worker {
	return &Worker{
		handler: handler,
	}
}

// Start begins processing jobs from the channel until it's closed or context is cancelled
func (w *Worker) Start(ctx context.Context, jobs <-chan map[string]string) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				// Channel closed
				return
			}
			// Process job, log errors but continue processing
			if err := w.handler(ctx, job); err != nil {
				log.Printf("worker: job handler error: %v", err)
			}
		}
	}
}
