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

		if w.persQueue != nil {
			// Persistence mode - status is already "processing" from dequeue
		} else if w.tracker != nil {
			w.tracker.MarkProcessing(envelope.ID)
		}

		jobCtx := ctx
		var cancel context.CancelFunc
		if envelope.Timeout > 0 {
			jobCtx, cancel = context.WithTimeout(ctx, envelope.Timeout)
		}

		err := w.registry.Handle(jobCtx, envelope)

		if cancel != nil {
			cancel()
		}

		if err == nil {
			if w.persQueue != nil {
				if completeErr := w.persQueue.CompleteJob(ctx, envelope.ID, envelope.Attempts); completeErr != nil {
					w.logger.Error("failed to mark job as completed in database",
						"error", completeErr,
						"job_id", envelope.ID)
					// Continue anyway - job executed successfully
				}
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
			if w.persQueue != nil {
				if dlqErr := w.persQueue.MoveToDLQ(ctx, envelope, err.Error()); dlqErr != nil {
					w.logger.Error("failed to move job to DLQ",
						"error", dlqErr,
						"job_id", envelope.ID,
						"original_error", err)
					// Continue anyway - we still want to log the original failure
				}
				w.logger.Error("job handler error, moved to DLQ",
					"error", err,
					"job_id", envelope.ID,
					"job_type", envelope.Type,
					"attempts", envelope.Attempts,
					"max_retries", envelope.MaxRetries)
			} else if w.tracker != nil {
				w.tracker.MarkFailed(envelope.ID, err)
				w.logger.Error("job handler error",
					"error", err,
					"job_id", envelope.ID,
					"job_type", envelope.Type,
					"attempts", envelope.Attempts,
					"max_retries", envelope.MaxRetries)
			} else {
				// No persistence or tracking, just log the error
				w.logger.Error("job handler error",
					"error", err,
					"job_id", envelope.ID,
					"job_type", envelope.Type,
					"attempts", envelope.Attempts,
					"max_retries", envelope.MaxRetries)
			}
			return
		}

		w.logger.Error("job handler error, will retry",
			"error", err,
			"job_id", envelope.ID,
			"job_type", envelope.Type,
			"attempts", envelope.Attempts,
			"max_retries", envelope.MaxRetries)

		if w.persQueue != nil {
			// In persistence mode, requeue immediately without blocking the worker
			// This allows workers to remain available to process other jobs
			if requeueErr := w.persQueue.RequeueJob(ctx, envelope); requeueErr != nil {
				w.logger.Error("failed to requeue job for retry",
					"error", requeueErr,
					"job_id", envelope.ID,
					"attempts", envelope.Attempts)
				// Job is stuck in "processing" state - will be recovered on restart
			}
			return // Exit retry loop - job will be picked up again from queue
		}

		// In-memory mode: retry after backoff delay
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
