package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

func TestWorker_ProcessesEnvelopes(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 1)
	registry := jobs.NewRegistry()

	var processedID jobs.JobID
	var processedType jobs.JobType
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		processedID = env.ID
		processedType = env.Type
		mu.Unlock()
		return nil
	})

	registry.Register("test", handler)

	worker := NewWorker(registry, nil, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	envelope := &jobs.Envelope{
		ID:     "test-123",
		Type:   "test",
		Status: "pending",
	}
	jobChan <- envelope
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if processedID != envelope.ID {
		t.Errorf("got ID %q, want %q", processedID, envelope.ID)
	}
	if processedType != envelope.Type {
		t.Errorf("got Type %q, want %q", processedType, envelope.Type)
	}
}

func TestWorker_HandlesMultipleJobTypes(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 10)
	registry := jobs.NewRegistry()

	var echoCount, emailCount int
	var mu sync.Mutex

	echoHandler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		echoCount++
		mu.Unlock()
		return nil
	})

	emailHandler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		emailCount++
		mu.Unlock()
		return nil
	})

	registry.Register("echo", echoHandler)
	registry.Register("email", emailHandler)

	worker := NewWorker(registry, nil, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	for i := 0; i < 3; i++ {
		jobChan <- &jobs.Envelope{
			ID:     jobs.JobID("echo-" + string(rune('a'+i))),
			Type:   "echo",
			Status: "pending",
		}
	}
	for i := 0; i < 2; i++ {
		jobChan <- &jobs.Envelope{
			ID:     jobs.JobID("email-" + string(rune('a'+i))),
			Type:   "email",
			Status: "pending",
		}
	}
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if echoCount != 3 {
		t.Errorf("processed %d echo jobs, want 3", echoCount)
	}
	if emailCount != 2 {
		t.Errorf("processed %d email jobs, want 2", emailCount)
	}
}

func TestWorker_HandlersReceiveCorrectPayloads(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 1)
	registry := jobs.NewRegistry()

	type EchoPayload struct {
		Message string `json:"message"`
	}

	var receivedMessage string
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		var payload EchoPayload
		if err := env.UnmarshalPayload(&payload); err != nil {
			return err
		}

		mu.Lock()
		receivedMessage = payload.Message
		mu.Unlock()
		return nil
	})

	registry.Register("echo", handler)

	worker := NewWorker(registry, nil, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	payloadData, _ := json.Marshal(EchoPayload{Message: "hello world"})
	envelope := &jobs.Envelope{
		ID:      "echo-payload",
		Type:    "echo",
		Payload: payloadData,
		Status:  "pending",
	}
	jobChan <- envelope
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if receivedMessage != "hello world" {
		t.Errorf("got message %q, want %q", receivedMessage, "hello world")
	}
}

func TestWorker_HandlesMultipleJobs(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 10)
	registry := jobs.NewRegistry()

	var count int
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})

	registry.Register("counter", handler)

	worker := NewWorker(registry, nil, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	for i := 0; i < 5; i++ {
		jobChan <- &jobs.Envelope{
			ID:     jobs.JobID("job-" + string(rune('a'+i))),
			Type:   "counter",
			Status: "pending",
		}
	}
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if count != 5 {
		t.Errorf("processed %d jobs, want 5", count)
	}
}

func TestWorker_RespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	jobChan := make(chan *jobs.Envelope, 10)
	registry := jobs.NewRegistry()

	var count int
	var mu sync.Mutex
	firstJobProcessed := make(chan struct{})
	var firstJobOnce sync.Once

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		mu.Unlock()

		firstJobOnce.Do(func() {
			close(firstJobProcessed)
		})

		<-ctx.Done()
		return nil
	})

	registry.Register("slow", handler)

	worker := NewWorker(registry, nil, nil)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	for i := 0; i < 10; i++ {
		jobChan <- &jobs.Envelope{
			ID:     jobs.JobID("slow-" + string(rune('a'+i))),
			Type:   "slow",
			Status: "pending",
		}
	}

	<-firstJobProcessed
	cancel()

	select {
	case <-done:
		// Worker stopped
	case <-time.After(50 * time.Millisecond):
		t.Fatal("worker did not stop after context cancellation")
	}

	mu.Lock()
	defer mu.Unlock()

	if count >= 10 {
		t.Errorf("processed %d jobs after cancellation, expected fewer", count)
	}
}

func TestWorker_StopsWhenChannelClosed(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 1)
	registry := jobs.NewRegistry()

	var count int
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		mu.Unlock()
		return nil
	})

	registry.Register("close", handler)

	worker := NewWorker(registry, nil, nil)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	jobChan <- &jobs.Envelope{
		ID:     "close-1",
		Type:   "close",
		Status: "pending",
	}
	close(jobChan)

	select {
	case <-done:
		// Worker stopped
	case <-time.After(10 * time.Millisecond):
		t.Fatal("worker did not stop after channel closed")
	}

	mu.Lock()
	defer mu.Unlock()

	if count != 1 {
		t.Errorf("processed %d jobs, want 1", count)
	}
}

func TestWorker_HandlesErrors(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 5)
	registry := jobs.NewRegistry()

	var count int
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		currentCount := count
		mu.Unlock()

		if currentCount%2 == 0 {
			return errors.New("simulated error")
		}
		return nil
	})

	registry.Register("errors", handler)

	worker := NewWorker(registry, nil, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	for i := 0; i < 5; i++ {
		jobChan <- &jobs.Envelope{
			ID:     jobs.JobID("err-" + string(rune('a'+i))),
			Type:   "errors",
			Status: "pending",
		}
	}
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if count != 5 {
		t.Errorf("processed %d jobs, want 5 (should continue after errors)", count)
	}
}

func TestWorker_UnknownJobType(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 2)
	registry := jobs.NewRegistry()

	var knownCount int
	var mu sync.Mutex

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		knownCount++
		mu.Unlock()
		return nil
	})

	registry.Register("known", handler)

	worker := NewWorker(registry, nil, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	jobChan <- &jobs.Envelope{
		ID:     "known-1",
		Type:   "known",
		Status: "pending",
	}
	jobChan <- &jobs.Envelope{
		ID:     "unknown-1",
		Type:   "unknown",
		Status: "pending",
	}
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if knownCount != 1 {
		t.Errorf("processed %d known jobs, want 1", knownCount)
	}
}

func TestWorker_LogsJobCompletionWithIDAndType(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 1)
	registry := jobs.NewRegistry()

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	})

	registry.Register("logged", handler)

	testLog := newTestLogger()
	logger := slog.New(testLog.handler())

	worker := NewWorker(registry, nil, logger)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	envelope := &jobs.Envelope{
		ID:     "logged-123",
		Type:   "logged",
		Status: "pending",
	}
	jobChan <- envelope
	close(jobChan)

	wg.Wait()

	if !testLog.hasMessage("job completed") {
		t.Error("expected 'job completed' log message")
	}

	if !testLog.hasAttr("job_id", jobs.JobID("logged-123")) {
		t.Error("expected log to include job_id attribute")
	}

	if !testLog.hasAttr("job_type", jobs.JobType("logged")) {
		t.Error("expected log to include job_type attribute")
	}
}

func TestWorker_LogsErrors(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 1)
	registry := jobs.NewRegistry()

	expectedErr := errors.New("handler error")
	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return expectedErr
	})

	registry.Register("error", handler)

	testLog := newTestLogger()
	logger := slog.New(testLog.handler())

	worker := NewWorker(registry, nil, logger)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	envelope := &jobs.Envelope{
		ID:     "error-123",
		Type:   "error",
		Status: "pending",
	}
	jobChan <- envelope
	close(jobChan)

	wg.Wait()

	if !testLog.hasMessage("job handler error") {
		t.Error("expected 'job handler error' log message")
	}

	if !testLog.hasAttr("job_id", jobs.JobID("error-123")) {
		t.Error("expected error log to include job_id attribute")
	}

	if !testLog.hasAttr("job_type", jobs.JobType("error")) {
		t.Error("expected error log to include job_type attribute")
	}
}

func TestWorker_MovesToDLQAfterMaxRetries(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 10) // Buffer for requeued jobs
	registry := jobs.NewRegistry()

	var dlqJobID jobs.JobID
	var dlqError string
	var failCount int
	var mu sync.Mutex
	dlqCalled := make(chan struct{})

	mockQueue := &mockPersistentQueue{
		requeueJobFunc: func(ctx context.Context, env *jobs.Envelope) error {
			// In persistence mode, requeue puts job back in channel for retry
			// Simulate by feeding it back
			go func() {
				jobChan <- env
			}()
			return nil
		},
		moveToDLQFunc: func(ctx context.Context, env *jobs.Envelope, errorMsg string) error {
			mu.Lock()
			dlqJobID = env.ID
			dlqError = errorMsg
			mu.Unlock()
			close(dlqCalled)
			return nil
		},
	}

	// Handler that always fails
	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		failCount++
		mu.Unlock()
		return errors.New("permanent failure")
	})

	registry.Register("failing", handler)

	worker := NewWorkerWithPersistence(registry, mockQueue, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	envelope := &jobs.Envelope{
		ID:         "dlq-test-123",
		Type:       "failing",
		Status:     "pending",
		MaxRetries: 3,
	}
	jobChan <- envelope

	// Wait for DLQ to be called
	<-dlqCalled
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if dlqJobID != envelope.ID {
		t.Errorf("expected DLQ job ID %q, got %q", envelope.ID, dlqJobID)
	}

	if dlqError == "" {
		t.Error("expected DLQ error message, got empty string")
	}

	if !strings.Contains(dlqError, "permanent failure") {
		t.Errorf("expected DLQ error to contain 'permanent failure', got %q", dlqError)
	}

	if failCount != 4 {
		t.Errorf("expected 4 attempts, got %d", failCount)
	}
}

func TestWorker_DLQPreservesErrorContext(t *testing.T) {
	ctx := context.Background()
	jobChan := make(chan *jobs.Envelope, 10)
	registry := jobs.NewRegistry()

	var capturedError string
	var capturedAttempts int
	var mu sync.Mutex
	dlqCalled := make(chan struct{})

	mockQueue := &mockPersistentQueue{
		requeueJobFunc: func(ctx context.Context, env *jobs.Envelope) error {
			go func() {
				jobChan <- env
			}()
			return nil
		},
		moveToDLQFunc: func(ctx context.Context, env *jobs.Envelope, errorMsg string) error {
			mu.Lock()
			capturedError = errorMsg
			mu.Unlock()
			close(dlqCalled)
			return nil
		},
	}

	// Handler that returns detailed error
	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		capturedAttempts = env.Attempts
		mu.Unlock()
		return fmt.Errorf("database connection failed: timeout after 30s")
	})

	registry.Register("db-job", handler)

	worker := NewWorkerWithPersistence(registry, mockQueue, nil)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx, jobChan)
	}()

	envelope := &jobs.Envelope{
		ID:         "db-job-456",
		Type:       "db-job",
		Status:     "pending",
		MaxRetries: 2,
	}
	jobChan <- envelope

	// Wait for DLQ to be called
	<-dlqCalled
	close(jobChan)

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if !strings.Contains(capturedError, "database connection failed") {
		t.Errorf("expected error to contain original message, got %q", capturedError)
	}

	if capturedAttempts != 3 {
		t.Errorf("expected 3 attempts, got %d", capturedAttempts)
	}
}

// Mock persistent queue for testing DLQ
type mockPersistentQueue struct {
	enqueueFunc     func(ctx context.Context, env *jobs.Envelope) error
	completeJobFunc func(ctx context.Context, jobID jobs.JobID) error
	failJobFunc     func(ctx context.Context, jobID jobs.JobID, errorMsg string) error
	requeueJobFunc  func(ctx context.Context, env *jobs.Envelope) error
	moveToDLQFunc   func(ctx context.Context, env *jobs.Envelope, errorMsg string) error
}

func (m *mockPersistentQueue) Enqueue(ctx context.Context, env *jobs.Envelope) error {
	if m.enqueueFunc != nil {
		return m.enqueueFunc(ctx, env)
	}
	return nil
}

func (m *mockPersistentQueue) Dequeue(ctx context.Context) (*jobs.Envelope, error) {
	return nil, nil
}

func (m *mockPersistentQueue) CompleteJob(ctx context.Context, jobID jobs.JobID) error {
	if m.completeJobFunc != nil {
		return m.completeJobFunc(ctx, jobID)
	}
	return nil
}

func (m *mockPersistentQueue) FailJob(ctx context.Context, jobID jobs.JobID, errorMsg string) error {
	if m.failJobFunc != nil {
		return m.failJobFunc(ctx, jobID, errorMsg)
	}
	return nil
}

func (m *mockPersistentQueue) RequeueJob(ctx context.Context, env *jobs.Envelope) error {
	if m.requeueJobFunc != nil {
		return m.requeueJobFunc(ctx, env)
	}
	return nil
}

func (m *mockPersistentQueue) MoveToDLQ(ctx context.Context, env *jobs.Envelope, errorMsg string) error {
	if m.moveToDLQFunc != nil {
		return m.moveToDLQFunc(ctx, env, errorMsg)
	}
	return nil
}

func (m *mockPersistentQueue) ListDLQJobs(ctx context.Context) []*queue.JobRecord {
	return nil
}

func (m *mockPersistentQueue) RequeueDLQJob(ctx context.Context, jobID jobs.JobID) error {
	return nil
}

func (m *mockPersistentQueue) GetJob(ctx context.Context, jobID jobs.JobID) (*queue.JobRecord, bool) {
	return nil, false
}

func (m *mockPersistentQueue) ListJobsByStatus(ctx context.Context, status string) []*queue.JobRecord {
	return nil
}

func (m *mockPersistentQueue) Close() error {
	return nil
}

func (m *mockPersistentQueue) CancelJob(ctx context.Context, jobID jobs.JobID) error {
	return nil
}

func (m *mockPersistentQueue) CreateCronJob(ctx context.Context, cronJob *cron.CronJob) error {
	return nil
}

func (m *mockPersistentQueue) GetCronJob(ctx context.Context, id cron.CronJobID) (*cron.CronJob, bool) {
	return nil, false
}

func (m *mockPersistentQueue) ListCronJobs(ctx context.Context) []*cron.CronJob {
	return nil
}

func (m *mockPersistentQueue) ListEnabledCronJobs(ctx context.Context) []*cron.CronJob {
	return nil
}

func (m *mockPersistentQueue) UpdateCronJob(ctx context.Context, cronJob *cron.CronJob) error {
	return nil
}

func (m *mockPersistentQueue) DeleteCronJob(ctx context.Context, id cron.CronJobID) error {
	return nil
}

func (m *mockPersistentQueue) UpdateCronJobNextRun(ctx context.Context, id cron.CronJobID, nextRun time.Time) error {
	return nil
}

func (m *mockPersistentQueue) UpdateCronJobLastRun(ctx context.Context, id cron.CronJobID, lastRun time.Time) error {
	return nil
}

func TestWorker_TimeoutEnforcement(t *testing.T) {
	t.Run("cancels job that exceeds timeout", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()

		jobStarted := make(chan struct{})
		jobCancelled := make(chan struct{})

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			close(jobStarted)
			<-ctx.Done()
			close(jobCancelled)
			return ctx.Err()
		})

		registry.Register("slow", handler)

		worker := NewWorker(registry, nil, nil)
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		envelope := &jobs.Envelope{
			ID:      "timeout-job",
			Type:    "slow",
			Timeout: 50 * time.Millisecond,
		}
		jobChan <- envelope
		close(jobChan)

		select {
		case <-jobStarted:
		case <-time.After(1 * time.Second):
			t.Fatal("job did not start")
		}

		select {
		case <-jobCancelled:
		case <-time.After(200 * time.Millisecond):
			t.Fatal("job was not cancelled after timeout")
		}

		wg.Wait()
	})

	t.Run("allows job without timeout to run indefinitely", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()

		jobCompleted := make(chan struct{})

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			time.Sleep(100 * time.Millisecond)
			close(jobCompleted)
			return nil
		})

		registry.Register("slow", handler)

		worker := NewWorker(registry, nil, nil)
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		envelope := &jobs.Envelope{
			ID:      "no-timeout-job",
			Type:    "slow",
			Timeout: 0,
		}
		jobChan <- envelope
		close(jobChan)

		select {
		case <-jobCompleted:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("job should have completed without timeout")
		}

		wg.Wait()
	})

	t.Run("timeout error is distinguishable from regular errors", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()

		var capturedError error
		var mu sync.Mutex

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			<-ctx.Done()
			return ctx.Err()
		})

		registry.Register("timeout", handler)

		mockQueue := &mockPersistentQueue{
			moveToDLQFunc: func(ctx context.Context, env *jobs.Envelope, errorMsg string) error {
				mu.Lock()
				capturedError = errors.New(errorMsg)
				mu.Unlock()
				return nil
			},
		}

		worker := NewWorkerWithPersistence(registry, mockQueue, nil)
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		envelope := &jobs.Envelope{
			ID:      "timeout-check",
			Type:    "timeout",
			Timeout: 10 * time.Millisecond,
		}
		jobChan <- envelope
		close(jobChan)

		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if capturedError == nil {
			t.Fatal("expected timeout error to be captured")
		}

		if capturedError.Error() != "context deadline exceeded" {
			t.Errorf("expected 'context deadline exceeded', got: %v", capturedError)
		}
	})
}

func TestWorker_TimeoutWithRetries(t *testing.T) {
	t.Run("timed out job goes through retry logic", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()

		attemptCount := 0
		var mu sync.Mutex

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			mu.Lock()
			attemptCount++
			mu.Unlock()
			<-ctx.Done()
			return ctx.Err()
		})

		registry.Register("timeout", handler)

		requeuedJobs := []*jobs.Envelope{}
		mockQueue := &mockPersistentQueue{
			requeueJobFunc: func(ctx context.Context, env *jobs.Envelope) error {
				mu.Lock()
				requeuedJobs = append(requeuedJobs, env)
				mu.Unlock()
				return nil
			},
		}

		worker := NewWorkerWithPersistence(registry, mockQueue, nil)
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		envelope := &jobs.Envelope{
			ID:         "retry-timeout",
			Type:       "timeout",
			Timeout:    10 * time.Millisecond,
			MaxRetries: 2,
			Attempts:   0,
		}
		jobChan <- envelope
		close(jobChan)

		time.Sleep(100 * time.Millisecond)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if attemptCount != 1 {
			t.Errorf("expected 1 attempt, got %d", attemptCount)
		}

		if len(requeuedJobs) == 0 {
			t.Fatal("expected job to be requeued after timeout")
		}

		if requeuedJobs[0].Attempts != 1 {
			t.Errorf("expected attempts=1 after requeue, got %d", requeuedJobs[0].Attempts)
		}
	})

	t.Run("timed out job moves to DLQ after max retries", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			<-ctx.Done()
			return ctx.Err()
		})

		registry.Register("timeout", handler)

		var dlqJobID jobs.JobID
		var dlqError string
		var mu sync.Mutex

		mockQueue := &mockPersistentQueue{
			moveToDLQFunc: func(ctx context.Context, env *jobs.Envelope, errorMsg string) error {
				mu.Lock()
				dlqJobID = env.ID
				dlqError = errorMsg
				mu.Unlock()
				return nil
			},
		}

		worker := NewWorkerWithPersistence(registry, mockQueue, nil)
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		envelope := &jobs.Envelope{
			ID:         "dlq-timeout",
			Type:       "timeout",
			Timeout:    10 * time.Millisecond,
			MaxRetries: 0,
			Attempts:   0,
		}
		jobChan <- envelope
		close(jobChan)

		time.Sleep(100 * time.Millisecond)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if dlqJobID != envelope.ID {
			t.Errorf("expected job %s in DLQ, got %s", envelope.ID, dlqJobID)
		}

		if dlqError == "" {
			t.Error("expected DLQ error message to be set")
		}

		if !strings.Contains(dlqError, "deadline") && !strings.Contains(dlqError, "timeout") {
			t.Errorf("expected timeout-related error, got: %s", dlqError)
		}
	})
}
