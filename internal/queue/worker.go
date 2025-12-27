package queue

import (
	"context"
	"log/slog"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type Worker struct {
	registry *jobs.Registry
	logger   *slog.Logger
}

func NewWorker(registry *jobs.Registry, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		registry: registry,
		logger:   logger,
	}
}

func (w *Worker) Start(ctx context.Context, jobs <-chan *jobs.Envelope) {
	for {
		select {
		case <-ctx.Done():
			return
		case envelope, ok := <-jobs:
			if !ok {
				return
			}

			if err := w.registry.Handle(ctx, envelope); err != nil {
				w.logger.Error("job handler error",
					"error", err,
					"job_id", envelope.ID,
					"job_type", envelope.Type)
			} else {
				w.logger.Info("job completed",
					"job_id", envelope.ID,
					"job_type", envelope.Type)
			}
		}
	}
}
