package shutdown

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// ShutdownTask is a function that performs cleanup during shutdown
type ShutdownTask func(ctx context.Context) error

// Manager coordinates graceful shutdown of application components
type Manager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	timeout      time.Duration
	tasks        []namedTask
	tasksMu      sync.RWMutex
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	errors       []error
	errorsMu     sync.Mutex
	shutdownFlag bool
	logger       *slog.Logger
	runningTasks sync.Map // tracks task name -> bool for incomplete task detection
}

type namedTask struct {
	name string
	task ShutdownTask
}

const defaultTimeout = 30 * time.Second

// NewManager creates a new shutdown manager with default timeout
func NewManager(ctx context.Context) *Manager {
	return NewManagerWithTimeout(ctx, defaultTimeout, nil)
}

// NewManagerWithTimeout creates a new shutdown manager with custom timeout
func NewManagerWithTimeout(ctx context.Context, timeout time.Duration, logger *slog.Logger) *Manager {
	shutdownCtx, cancel := context.WithCancel(ctx)

	if logger == nil {
		logger = slog.Default()
	}

	return &Manager{
		ctx:          shutdownCtx,
		cancel:       cancel,
		timeout:      timeout,
		tasks:        make([]namedTask, 0),
		shutdownDone: make(chan struct{}),
		logger:       logger,
	}
}

// RegisterTask registers a task to be executed during shutdown
func (m *Manager) RegisterTask(name string, task ShutdownTask) error {
	m.tasksMu.Lock()
	defer m.tasksMu.Unlock()

	if m.shutdownFlag {
		return errors.New("cannot register task after shutdown has started")
	}

	m.tasks = append(m.tasks, namedTask{
		name: name,
		task: task,
	})
	return nil
}

// Shutdown initiates graceful shutdown (idempotent)
func (m *Manager) Shutdown() {
	m.shutdownOnce.Do(func() {
		m.tasksMu.Lock()
		m.shutdownFlag = true
		m.tasksMu.Unlock()

		// Execute shutdown tasks (this will create and manage task context)
		m.executeTasks()

		// Cancel the manager context after tasks complete
		// This signals to external watchers that shutdown is fully complete
		m.cancel()
		close(m.shutdownDone)
	})
}

// executeTasks runs all registered tasks concurrently with timeout
func (m *Manager) executeTasks() {
	m.tasksMu.RLock()
	tasks := m.tasks
	m.tasksMu.RUnlock()

	if len(tasks) == 0 {
		return
	}

	// Create context with timeout for task execution
	// This is independent of the manager's context to allow tasks time to complete
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	// Immediately cancel the context to signal tasks that shutdown is happening
	// Tasks can check ctx.Done() to know they should wrap up
	// But they still have the timeout period to finish their work
	cancel()

	var wg sync.WaitGroup

	// Track running tasks for timeout reporting
	for _, nt := range tasks {
		m.runningTasks.Store(nt.name, true)
		wg.Add(1)
		go func(nt namedTask) {
			defer wg.Done()
			defer m.runningTasks.Delete(nt.name)

			if err := nt.task(ctx); err != nil {
				m.recordError(err)
			}
		}(nt)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(m.timeout)
	defer timer.Stop()

	select {
	case <-done:
		// All tasks completed successfully
		m.logger.Info("shutdown completed", "tasks_completed", len(tasks))
	case <-timer.C:
		// Timeout occurred - collect incomplete tasks
		incomplete := []string{}
		m.runningTasks.Range(func(key, value interface{}) bool {
			incomplete = append(incomplete, key.(string))
			return true
		})
		m.logger.Warn("shutdown timeout exceeded",
			"timeout", m.timeout,
			"total_tasks", len(tasks),
			"incomplete_tasks", incomplete)
	}
}

// recordError safely records an error from a task
func (m *Manager) recordError(err error) {
	m.errorsMu.Lock()
	defer m.errorsMu.Unlock()
	m.errors = append(m.errors, err)
}

// Wait blocks until shutdown is complete
func (m *Manager) Wait() {
	<-m.shutdownDone
}

// Context returns the shutdown context
func (m *Manager) Context() context.Context {
	return m.ctx
}

// Errors returns all errors collected during shutdown
func (m *Manager) Errors() []error {
	m.errorsMu.Lock()
	defer m.errorsMu.Unlock()

	// Return a copy to prevent external modification
	errorsCopy := make([]error, len(m.errors))
	copy(errorsCopy, m.errors)
	return errorsCopy
}
