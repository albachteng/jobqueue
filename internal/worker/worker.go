package worker

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/tracking"
)

type BackoffFunc func(attempt int) time.Duration

type Worker struct {
	registry  *jobs.Registry
	tracker   *tracking.JobTracker
	persQueue queue.PersistentQueue
	logger    *slog.Logger
	backoffFn BackoffFunc
}

func NewWorker(registry *jobs.Registry, tracker *tracking.JobTracker, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		registry:  registry,
		tracker:   tracker,
		logger:    logger,
		backoffFn: calculateBackoff,
	}
}

func NewWorkerWithPersistence(registry *jobs.Registry, persQueue queue.PersistentQueue, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		registry:  registry,
		persQueue: persQueue,
		logger:    logger,
		backoffFn: calculateBackoff,
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

			w.processWithRetry(ctx, envelope)
		}
	}
}

func (w *Worker) processWithRetry(ctx context.Context, envelope *jobs.Envelope) {
	for {
		envelope.Attempts++

		// Mark as processing
		if w.persQueue != nil {
			// Persistence mode - status is already "processing" from dequeue
		} else if w.tracker != nil {
			w.tracker.MarkProcessing(envelope.ID)
		}

		err := w.registry.Handle(ctx, envelope)

		if err == nil {
			// Job completed successfully
			if w.persQueue != nil {
				w.persQueue.CompleteJob(ctx, envelope.ID)
			} else if w.tracker != nil {
				w.tracker.MarkCompleted(envelope.ID)
			}
			w.logger.Info("job completed",
				"job_id", envelope.ID,
				"job_type", envelope.Type,
				"attempts", envelope.Attempts)
			return
		}

		shouldRetry := envelope.Attempts <= envelope.MaxRetries

		if !shouldRetry {
			// Job failed permanently
			if w.persQueue != nil {
				w.persQueue.FailJob(ctx, envelope.ID, err.Error())
			} else if w.tracker != nil {
				w.tracker.MarkFailed(envelope.ID, err)
			}
			w.logger.Error("job handler error",
				"error", err,
				"job_id", envelope.ID,
				"job_type", envelope.Type,
				"attempts", envelope.Attempts,
				"max_retries", envelope.MaxRetries)
			return
		}

		w.logger.Error("job handler error, will retry",
			"error", err,
			"job_id", envelope.ID,
			"job_type", envelope.Type,
			"attempts", envelope.Attempts,
			"max_retries", envelope.MaxRetries)

		// Requeue for retry with updated attempt count
		if w.persQueue != nil {
			backoffDelay := w.backoffFn(envelope.Attempts - 1)
			time.Sleep(backoffDelay)
			w.persQueue.RequeueJob(ctx, envelope)
			return // Exit retry loop - job will be picked up again from queue
		}

		// In-memory mode: retry immediately after backoff
		backoffDelay := w.backoffFn(envelope.Attempts - 1)
		time.Sleep(backoffDelay)
	}
}

func calculateBackoff(attempt int) time.Duration {
	const (
		baseDelay = 100 * time.Millisecond
		maxDelay  = 30 * time.Second
	)

	delay := float64(baseDelay) * math.Pow(2, float64(attempt))

	if delay > float64(maxDelay) {
		return maxDelay
	}

	return time.Duration(delay)
}
