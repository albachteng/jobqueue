package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/tracking"
)

type Dispatcher struct {
	queue      queue.Queue[*jobs.Envelope]
	numWorkers int
	registry   *jobs.Registry
	tracker    *tracking.JobTracker
	logger     *slog.Logger
	jobChan    chan *jobs.Envelope
	wg         sync.WaitGroup
}

func NewDispatcher(queue queue.Queue[*jobs.Envelope], numWorkers int, registry *jobs.Registry, tracker *tracking.JobTracker, logger *slog.Logger) *Dispatcher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{
		queue:      queue,
		numWorkers: numWorkers,
		registry:   registry,
		tracker:    tracker,
		logger:     logger,
	}
}

func (d *Dispatcher) Start(ctx context.Context) error {
	d.logger.Info("dispatcher starting", "num_workers", d.numWorkers)
	d.jobChan = make(chan *jobs.Envelope, d.numWorkers)

	// Check if queue is persistent
	persQueue, isPersistent := d.queue.(queue.PersistentQueue)

	for i := 0; i < d.numWorkers; i++ {
		d.wg.Add(1)
		go func(workerID int) {
			defer d.wg.Done()
			var worker *Worker
			if isPersistent {
				worker = NewWorkerWithPersistence(d.registry, persQueue, d.logger.With("worker_id", workerID))
			} else {
				worker = NewWorker(d.registry, d.tracker, d.logger.With("worker_id", workerID))
			}
			worker.Start(ctx, d.jobChan)
		}(i)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(d.jobChan)
				return
			default:
				job, err := d.queue.Dequeue(ctx)
				if err != nil {
					if errors.Is(err, queue.ErrEmptyQueue) {
						time.Sleep(10 * time.Millisecond)
						continue
					}
					close(d.jobChan)
					return
				}
				d.jobChan <- job
			}
		}
	}()

	return nil
}

func (d *Dispatcher) Stop() {
	d.logger.Info("dispatcher stopping")
	d.wg.Wait()
	d.logger.Info("dispatcher stopped")
}
