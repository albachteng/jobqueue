package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/tracking"
)

// testBackoff returns a minimal delay for testing
func testBackoff(attempt int) time.Duration {
	return 1 * time.Millisecond
}

func TestWorker_RetryOnFailure(t *testing.T) {
	t.Run("retries failed job up to MaxRetries", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()
		tracker := tracking.NewJobTracker()

		attemptCount := 0
		var mu sync.Mutex

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			mu.Lock()
			attemptCount++
			mu.Unlock()
			return errors.New("simulated failure")
		})

		registry.Register("retry-test", handler)
		worker := NewWorker(registry, tracker, nil)
		worker.backoffFn = testBackoff

		envelope := &jobs.Envelope{
			ID:         "retry-123",
			Type:       "retry-test",
			Status:     "pending",
			MaxRetries: 3,
			Attempts:   0,
		}
		tracker.Register(envelope)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		jobChan <- envelope
		close(jobChan)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if attemptCount != 4 {
			t.Errorf("got %d attempts, want 4 (1 initial + 3 retries)", attemptCount)
		}

		info, _ := tracker.Get("retry-123")
		if info.Status != tracking.StatusFailed {
			t.Errorf("got status %q, want %q", info.Status, tracking.StatusFailed)
		}

		if info.Envelope.Attempts != 4 {
			t.Errorf("got %d attempts in envelope, want 4", info.Envelope.Attempts)
		}
	})

	t.Run("succeeds on retry", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()
		tracker := tracking.NewJobTracker()

		attemptCount := 0
		var mu sync.Mutex

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			mu.Lock()
			attemptCount++
			currentAttempt := attemptCount
			mu.Unlock()

			if currentAttempt < 3 {
				return errors.New("simulated failure")
			}
			return nil
		})

		registry.Register("eventual-success", handler)
		worker := NewWorker(registry, tracker, nil)
		worker.backoffFn = testBackoff

		envelope := &jobs.Envelope{
			ID:         "eventual-success-123",
			Type:       "eventual-success",
			Status:     "pending",
			MaxRetries: 5,
			Attempts:   0,
		}
		tracker.Register(envelope)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		jobChan <- envelope
		close(jobChan)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if attemptCount != 3 {
			t.Errorf("got %d attempts, want 3", attemptCount)
		}

		info, _ := tracker.Get("eventual-success-123")
		if info.Status != tracking.StatusCompleted {
			t.Errorf("got status %q, want %q", info.Status, tracking.StatusCompleted)
		}
	})

	t.Run("does not retry when MaxRetries is 0", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()
		tracker := tracking.NewJobTracker()

		attemptCount := 0
		var mu sync.Mutex

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			mu.Lock()
			attemptCount++
			mu.Unlock()
			return errors.New("simulated failure")
		})

		registry.Register("no-retry", handler)
		worker := NewWorker(registry, tracker, nil)
		worker.backoffFn = testBackoff

		envelope := &jobs.Envelope{
			ID:         "no-retry-123",
			Type:       "no-retry",
			Status:     "pending",
			MaxRetries: 0,
			Attempts:   0,
		}
		tracker.Register(envelope)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		jobChan <- envelope
		close(jobChan)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if attemptCount != 1 {
			t.Errorf("got %d attempts, want 1 (no retries)", attemptCount)
		}

		info, _ := tracker.Get("no-retry-123")
		if info.Status != tracking.StatusFailed {
			t.Errorf("got status %q, want %q", info.Status, tracking.StatusFailed)
		}
	})

	t.Run("successful job does not retry", func(t *testing.T) {
		ctx := context.Background()
		jobChan := make(chan *jobs.Envelope, 1)
		registry := jobs.NewRegistry()
		tracker := tracking.NewJobTracker()

		attemptCount := 0
		var mu sync.Mutex

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			mu.Lock()
			attemptCount++
			mu.Unlock()
			return nil
		})

		registry.Register("success", handler)
		worker := NewWorker(registry, tracker, nil)
		worker.backoffFn = testBackoff

		envelope := &jobs.Envelope{
			ID:         "success-123",
			Type:       "success",
			Status:     "pending",
			MaxRetries: 3,
			Attempts:   0,
		}
		tracker.Register(envelope)

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Start(ctx, jobChan)
		}()

		jobChan <- envelope
		close(jobChan)
		wg.Wait()

		mu.Lock()
		defer mu.Unlock()

		if attemptCount != 1 {
			t.Errorf("got %d attempts, want 1 (no retries on success)", attemptCount)
		}

		info, _ := tracker.Get("success-123")
		if info.Status != tracking.StatusCompleted {
			t.Errorf("got status %q, want %q", info.Status, tracking.StatusCompleted)
		}
	})
}

func TestCalculateBackoff(t *testing.T) {
	t.Run("returns exponential backoff delays", func(t *testing.T) {
		testCases := []struct {
			attempt  int
			expected time.Duration
		}{
			{0, 100 * time.Millisecond},   // 2^0 * 100ms = 100ms
			{1, 200 * time.Millisecond},   // 2^1 * 100ms = 200ms
			{2, 400 * time.Millisecond},   // 2^2 * 100ms = 400ms
			{3, 800 * time.Millisecond},   // 2^3 * 100ms = 800ms
			{4, 1600 * time.Millisecond},  // 2^4 * 100ms = 1600ms
			{5, 3200 * time.Millisecond},  // 2^5 * 100ms = 3200ms
			{6, 6400 * time.Millisecond},  // 2^6 * 100ms = 6400ms
			{7, 12800 * time.Millisecond}, // 2^7 * 100ms = 12800ms
		}

		for _, tc := range testCases {
			delay := calculateBackoff(tc.attempt)
			if delay != tc.expected {
				t.Errorf("attempt %d: got %v, want %v", tc.attempt, delay, tc.expected)
			}
		}
	})

	t.Run("caps backoff at maximum delay", func(t *testing.T) {
		maxDelay := 30 * time.Second

		// Test attempts that should be capped
		testCases := []int{9, 10, 15, 20, 100}

		for _, attempt := range testCases {
			delay := calculateBackoff(attempt)
			if delay != maxDelay {
				t.Errorf("attempt %d: got %v, want %v (capped)", attempt, delay, maxDelay)
			}
		}
	})
}

func TestJobTracker_RetryTracking(t *testing.T) {
	t.Run("tracks retry attempts in envelope", func(t *testing.T) {
		tracker := tracking.NewJobTracker()
		envelope := &jobs.Envelope{
			ID:         "track-retry-123",
			Type:       "test",
			Status:     "pending",
			MaxRetries: 3,
			Attempts:   0,
		}
		tracker.Register(envelope)

		envelope.Attempts = 1
		tracker.MarkProcessing(envelope.ID)
		tracker.MarkFailed(envelope.ID, errors.New("attempt 1 failed"))

		info, _ := tracker.Get(envelope.ID)
		if info.Envelope.Attempts != 1 {
			t.Errorf("got %d attempts, want 1", info.Envelope.Attempts)
		}

		envelope.Attempts = 2
		tracker.MarkProcessing(envelope.ID)
		tracker.MarkFailed(envelope.ID, errors.New("attempt 2 failed"))

		info, _ = tracker.Get(envelope.ID)
		if info.Envelope.Attempts != 2 {
			t.Errorf("got %d attempts, want 2", info.Envelope.Attempts)
		}
	})

	t.Run("distinguishes retried failures from permanent failures", func(t *testing.T) {
		tracker := tracking.NewJobTracker()

		retryJob := &jobs.Envelope{
			ID:         "will-retry",
			Type:       "test",
			MaxRetries: 3,
			Attempts:   1,
		}
		tracker.Register(retryJob)
		tracker.MarkFailed(retryJob.ID, errors.New("will retry"))

		failedJob := &jobs.Envelope{
			ID:         "exhausted",
			Type:       "test",
			MaxRetries: 3,
			Attempts:   4, // Exceeded max retries
		}
		tracker.Register(failedJob)
		tracker.MarkFailed(failedJob.ID, errors.New("exhausted retries"))

		retryInfo, _ := tracker.Get("will-retry")
		failedInfo, _ := tracker.Get("exhausted")

		if retryInfo.Envelope.Attempts >= retryInfo.Envelope.MaxRetries {
			t.Error("retry job should not have exhausted retries")
		}

		if failedInfo.Envelope.Attempts <= failedInfo.Envelope.MaxRetries {
			t.Error("failed job should have exhausted retries")
		}
	})
}
