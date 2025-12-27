package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
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

	handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		mu.Lock()
		count++
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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

	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Worker stopped
	case <-time.After(100 * time.Millisecond):
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
	case <-time.After(100 * time.Millisecond):
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
