package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Dispatcher pulls jobs from a queue and distributes them to a worker pool
type Dispatcher struct {
	queue      Queue[map[string]string]
	numWorkers int
	handler    HandlerFunc
	jobChan    chan map[string]string
	wg         sync.WaitGroup
}

func NewDispatcher(queue Queue[map[string]string], numWorkers int, handler HandlerFunc) *Dispatcher {
	return &Dispatcher{
		queue:      queue,
		numWorkers: numWorkers,
		handler:    handler,
	}
}

// Start begins the dispatcher - pulls jobs from queue and sends to workers
func (d *Dispatcher) Start(ctx context.Context) error {
	d.jobChan = make(chan map[string]string, d.numWorkers)

	for i := 0; i < d.numWorkers; i++ {
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			worker := NewWorker(d.handler)
			worker.Start(ctx, d.jobChan)
		}()
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
					// Other error (context cancelled, etc)
					close(d.jobChan)
					return
				}
				d.jobChan <- job
			}
		}
	}()

	return nil
}

// Stop gracefully shuts down the dispatcher, waiting for in-flight jobs to complete
func (d *Dispatcher) Stop() {
	d.wg.Wait()
}
