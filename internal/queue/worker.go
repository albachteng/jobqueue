package queue

import (
	"context"
	"log/slog"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type Worker struct {
	registry *jobs.Registry
	tracker  *jobs.JobTracker
	logger   *slog.Logger
}

func NewWorker(registry *jobs.Registry, tracker *jobs.JobTracker, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		registry: registry,
		tracker:  tracker,
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

			if w.tracker != nil {
				w.tracker.MarkProcessing(envelope.ID)
			}

			if err := w.registry.Handle(ctx, envelope); err != nil {
				if w.tracker != nil {
					w.tracker.MarkFailed(envelope.ID, err)
				}
				w.logger.Error("job handler error",
					"error", err,
					"job_id", envelope.ID,
					"job_type", envelope.Type)
			} else {
				if w.tracker != nil {
					w.tracker.MarkCompleted(envelope.ID)
				}
				w.logger.Info("job completed",
					"job_id", envelope.ID,
					"job_type", envelope.Type)
			}
		}
	}
}
