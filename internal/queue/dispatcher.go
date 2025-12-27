package queue

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type Dispatcher struct {
	queue      Queue[*jobs.Envelope]
	numWorkers int
	registry   *jobs.Registry
	tracker    *jobs.JobTracker
	logger     *slog.Logger
	jobChan    chan *jobs.Envelope
	wg         sync.WaitGroup
}

func NewDispatcher(queue Queue[*jobs.Envelope], numWorkers int, registry *jobs.Registry, tracker *jobs.JobTracker, logger *slog.Logger) *Dispatcher {
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

	for i := 0; i < d.numWorkers; i++ {
		d.wg.Add(1)
		go func(workerID int) {
			defer d.wg.Done()
			worker := NewWorker(d.registry, d.tracker, d.logger.With("worker_id", workerID))
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
					if errors.Is(err, ErrEmptyQueue) {
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
