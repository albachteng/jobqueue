package queue

import (
	"context"
	"log/slog"
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
	logger  *slog.Logger
}

func NewWorker(handler HandlerFunc, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		handler: handler,
		logger:  logger,
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
				w.logger.Error("job handler error",
					"error", err,
					"job", job)
			}
		}
	}
}
