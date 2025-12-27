package shutdown

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestShutdownManager_SignalHandling(t *testing.T) {
	t.Run("handles SIGTERM signal", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		shutdownComplete := make(chan struct{})
		go func() {
			mgr.Wait()
			close(shutdownComplete)
		}()

		mgr.Shutdown()

		select {
		case <-shutdownComplete:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Error("shutdown did not complete in time")
		}
	})

	t.Run("handles SIGINT signal", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		shutdownComplete := make(chan struct{})
		go func() {
			mgr.Wait()
			close(shutdownComplete)
		}()

		mgr.Shutdown()

		select {
		case <-shutdownComplete:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Error("shutdown did not complete in time")
		}
	})

	t.Run("handles multiple signals gracefully", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		shutdownComplete := make(chan struct{})
		go func() {
			mgr.Wait()
			close(shutdownComplete)
		}()

		mgr.Shutdown()
		mgr.Shutdown()
		mgr.Shutdown()

		select {
		case <-shutdownComplete:
			// Success - should only shutdown once
		case <-time.After(100 * time.Millisecond):
			t.Error("shutdown did not complete in time")
		}
	})
}

func TestShutdownManager_GracefulPeriod(t *testing.T) {
	t.Run("waits for registered tasks to complete", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		taskCompleted := atomic.Bool{}

		mgr.RegisterTask("test-task", func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			taskCompleted.Store(true)
			return nil
		})

		go mgr.Shutdown()
		mgr.Wait()

		if !taskCompleted.Load() {
			t.Error("task did not complete before shutdown finished")
		}
	})

	t.Run("completes multiple tasks concurrently", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		completedCount := atomic.Int32{}

		for i := 0; i < 5; i++ {
			mgr.RegisterTask("test-task", func(ctx context.Context) error {
				time.Sleep(30 * time.Millisecond)
				completedCount.Add(1)
				return nil
			})
		}

		go mgr.Shutdown()
		mgr.Wait()

		if completedCount.Load() != 5 {
			t.Errorf("got %d completed tasks, want 5", completedCount.Load())
		}
	})

	t.Run("respects shutdown timeout", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManagerWithTimeout(ctx, 100*time.Millisecond)

		taskCompleted := atomic.Bool{}

		mgr.RegisterTask("slow-task", func(ctx context.Context) error {
			select {
			case <-time.After(500 * time.Millisecond):
				taskCompleted.Store(true)
				return nil
			case <-ctx.Done():
				// Context cancelled due to timeout
				return ctx.Err()
			}
		})

		start := time.Now()

		go mgr.Shutdown()
		mgr.Wait()

		elapsed := time.Since(start)

		if elapsed > 200*time.Millisecond {
			t.Errorf("shutdown took %v, expected ~100ms timeout", elapsed)
		}

		if taskCompleted.Load() {
			t.Error("task should have been cancelled by timeout")
		}
	})
}

func TestShutdownManager_ContextCancellation(t *testing.T) {
	t.Run("cancels context on shutdown", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		shutdownCtx := mgr.Context()

		select {
		case <-shutdownCtx.Done():
			t.Error("context should not be cancelled initially")
		default:
			// Good
		}

		mgr.Shutdown()

		select {
		case <-shutdownCtx.Done():
			// Good
		case <-time.After(100 * time.Millisecond):
			t.Error("context was not cancelled after shutdown")
		}
	})

	t.Run("tasks receive cancelled context", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		contextCancelled := atomic.Bool{}

		mgr.RegisterTask("context-check", func(ctx context.Context) error {
			<-ctx.Done()
			contextCancelled.Store(true)
			return ctx.Err()
		})

		go mgr.Shutdown()
		mgr.Wait()

		if !contextCancelled.Load() {
			t.Error("task context was not cancelled")
		}
	})
}

func TestShutdownManager_ErrorHandling(t *testing.T) {
	t.Run("collects errors from tasks", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		mgr.RegisterTask("failing-task-1", func(ctx context.Context) error {
			return context.DeadlineExceeded
		})

		mgr.RegisterTask("failing-task-2", func(ctx context.Context) error {
			return context.Canceled
		})

		mgr.RegisterTask("successful-task", func(ctx context.Context) error {
			return nil
		})

		go mgr.Shutdown()
		mgr.Wait()

		errors := mgr.Errors()
		if len(errors) != 2 {
			t.Errorf("got %d errors, want 2", len(errors))
		}
	})
}

func TestShutdownManager_Integration(t *testing.T) {
	t.Run("realistic shutdown scenario", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManagerWithTimeout(ctx, 2*time.Second)

		httpServerStopped := atomic.Bool{}
		dispatcherStopped := atomic.Bool{}
		workersCompleted := atomic.Int32{}

		mgr.RegisterTask("http-server", func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond) // Simulate in-flight requests
			httpServerStopped.Store(true)
			return nil
		})

		mgr.RegisterTask("dispatcher", func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond) // Simulate finishing jobs
			dispatcherStopped.Store(true)
			return nil
		})

		for i := 0; i < 3; i++ {
			mgr.RegisterTask("worker", func(ctx context.Context) error {
				time.Sleep(75 * time.Millisecond) // Simulate job processing
				workersCompleted.Add(1)
				return nil
			})
		}

		start := time.Now()

		go mgr.Shutdown()
		mgr.Wait()

		elapsed := time.Since(start)

		if !httpServerStopped.Load() {
			t.Error("HTTP server did not stop")
		}
		if !dispatcherStopped.Load() {
			t.Error("dispatcher did not stop")
		}
		if workersCompleted.Load() != 3 {
			t.Errorf("got %d workers completed, want 3", workersCompleted.Load())
		}

		// Longest task is 100ms, so with some overhead should be < 200ms
		if elapsed > 200*time.Millisecond {
			t.Errorf("shutdown took %v, expected < 200ms (tasks should run concurrently)", elapsed)
		}
	})
}

func TestShutdownManager_NoTasks(t *testing.T) {
	t.Run("handles shutdown with no registered tasks", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		// Should not hang or panic
		go mgr.Shutdown()

		done := make(chan struct{})
		go func() {
			mgr.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(100 * time.Millisecond):
			t.Error("shutdown with no tasks did not complete")
		}
	})
}

func TestShutdownManager_RegisterAfterShutdown(t *testing.T) {
	t.Run("prevents task registration after shutdown started", func(t *testing.T) {
		ctx := context.Background()
		mgr := NewManager(ctx)

		mgr.Shutdown()

		err := mgr.RegisterTask("late-task", func(ctx context.Context) error {
			return nil
		})

		if err == nil {
			t.Error("expected error when registering task after shutdown")
		}
	})
}
