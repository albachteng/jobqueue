package queue

import "context"

// Dispatcher pulls jobs from a queue and distributes them to a worker pool
type Dispatcher struct {
	queue      Queue[map[string]string]
	numWorkers int
	handler    HandlerFunc
}

// NewDispatcher creates a new dispatcher with the given queue, worker count, and handler
func NewDispatcher(queue Queue[map[string]string], numWorkers int, handler HandlerFunc) *Dispatcher {
	return &Dispatcher{
		queue:      queue,
		numWorkers: numWorkers,
		handler:    handler,
	}
}

// Start begins the dispatcher - pulls jobs from queue and sends to workers
func (d *Dispatcher) Start(ctx context.Context) error {
	// TODO: implement
	return nil
}

// Stop gracefully shuts down the dispatcher, waiting for in-flight jobs to complete
func (d *Dispatcher) Stop() {
	// TODO: implement
}
