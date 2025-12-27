package jobs

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestJobTracker_Register(t *testing.T) {
	t.Run("registers job with pending status", func(t *testing.T) {
		tracker := NewJobTracker()
		envelope := &Envelope{
			ID:   "test-123",
			Type: "test",
		}

		tracker.Register(envelope)

		info, exists := tracker.Get("test-123")
		if !exists {
			t.Fatal("expected job to be registered")
		}

		if info.Status != StatusPending {
			t.Errorf("got status %q, want %q", info.Status, StatusPending)
		}

		if info.Envelope.ID != envelope.ID {
			t.Errorf("got ID %q, want %q", info.Envelope.ID, envelope.ID)
		}
	})

	t.Run("registers multiple jobs", func(t *testing.T) {
		tracker := NewJobTracker()

		for i := 0; i < 5; i++ {
			envelope := &Envelope{
				ID:   JobID("job-" + string(rune('a'+i))),
				Type: "test",
			}
			tracker.Register(envelope)
		}

		jobs := tracker.List()
		if len(jobs) != 5 {
			t.Errorf("got %d jobs, want 5", len(jobs))
		}
	})
}

func TestJobTracker_Get(t *testing.T) {
	t.Run("returns job info for existing job", func(t *testing.T) {
		tracker := NewJobTracker()
		envelope := &Envelope{
			ID:   "exists-123",
			Type: "test",
		}
		tracker.Register(envelope)

		info, exists := tracker.Get("exists-123")

		if !exists {
			t.Fatal("expected job to exist")
		}

		if info.Envelope.ID != "exists-123" {
			t.Errorf("got ID %q, want %q", info.Envelope.ID, "exists-123")
		}
	})

	t.Run("returns false for non-existent job", func(t *testing.T) {
		tracker := NewJobTracker()

		_, exists := tracker.Get("non-existent")

		if exists {
			t.Error("expected job to not exist")
		}
	})
}

func TestJobTracker_MarkProcessing(t *testing.T) {
	t.Run("updates status to processing", func(t *testing.T) {
		tracker := NewJobTracker()
		envelope := &Envelope{
			ID:   "proc-123",
			Type: "test",
		}
		tracker.Register(envelope)

		before := time.Now()
		tracker.MarkProcessing("proc-123")
		after := time.Now()

		info, _ := tracker.Get("proc-123")

		if info.Status != StatusProcessing {
			t.Errorf("got status %q, want %q", info.Status, StatusProcessing)
		}

		if info.StartedAt == nil {
			t.Fatal("expected StartedAt to be set")
		}

		if info.StartedAt.Before(before) || info.StartedAt.After(after) {
			t.Errorf("StartedAt %v not within expected range [%v, %v]",
				info.StartedAt, before, after)
		}
	})

	t.Run("does nothing for non-existent job", func(t *testing.T) {
		tracker := NewJobTracker()
		tracker.MarkProcessing("non-existent")
	})
}

func TestJobTracker_MarkCompleted(t *testing.T) {
	t.Run("updates status to completed", func(t *testing.T) {
		tracker := NewJobTracker()
		envelope := &Envelope{
			ID:   "complete-123",
			Type: "test",
		}
		tracker.Register(envelope)
		tracker.MarkProcessing("complete-123")

		before := time.Now()
		tracker.MarkCompleted("complete-123")
		after := time.Now()

		info, _ := tracker.Get("complete-123")

		if info.Status != StatusCompleted {
			t.Errorf("got status %q, want %q", info.Status, StatusCompleted)
		}

		if info.FinishedAt == nil {
			t.Fatal("expected FinishedAt to be set")
		}

		if info.FinishedAt.Before(before) || info.FinishedAt.After(after) {
			t.Errorf("FinishedAt %v not within expected range [%v, %v]",
				info.FinishedAt, before, after)
		}
	})

	t.Run("does nothing for non-existent job", func(t *testing.T) {
		tracker := NewJobTracker()
		tracker.MarkCompleted("non-existent")
	})
}

func TestJobTracker_MarkFailed(t *testing.T) {
	t.Run("updates status to failed with error", func(t *testing.T) {
		tracker := NewJobTracker()
		envelope := &Envelope{
			ID:   "failed-123",
			Type: "test",
		}
		tracker.Register(envelope)
		tracker.MarkProcessing("failed-123")

		testErr := errors.New("something went wrong")
		before := time.Now()
		tracker.MarkFailed("failed-123", testErr)
		after := time.Now()

		info, _ := tracker.Get("failed-123")

		if info.Status != StatusFailed {
			t.Errorf("got status %q, want %q", info.Status, StatusFailed)
		}

		if info.Error != "something went wrong" {
			t.Errorf("got error %q, want %q", info.Error, "something went wrong")
		}

		if info.FinishedAt == nil {
			t.Fatal("expected FinishedAt to be set")
		}

		if info.FinishedAt.Before(before) || info.FinishedAt.After(after) {
			t.Errorf("FinishedAt %v not within expected range [%v, %v]",
				info.FinishedAt, before, after)
		}
	})

	t.Run("does nothing for non-existent job", func(t *testing.T) {
		tracker := NewJobTracker()
		tracker.MarkFailed("non-existent", errors.New("test"))
	})
}

func TestJobTracker_List(t *testing.T) {
	t.Run("returns all jobs", func(t *testing.T) {
		tracker := NewJobTracker()

		tracker.Register(&Envelope{ID: "job-1", Type: "test"})
		tracker.Register(&Envelope{ID: "job-2", Type: "test"})
		tracker.Register(&Envelope{ID: "job-3", Type: "test"})

		jobs := tracker.List()

		if len(jobs) != 3 {
			t.Errorf("got %d jobs, want 3", len(jobs))
		}
	})

	t.Run("returns empty list when no jobs", func(t *testing.T) {
		tracker := NewJobTracker()

		jobs := tracker.List()

		if len(jobs) != 0 {
			t.Errorf("got %d jobs, want 0", len(jobs))
		}
	})
}

func TestJobTracker_ListByStatus(t *testing.T) {
	t.Run("filters jobs by status", func(t *testing.T) {
		tracker := NewJobTracker()

		tracker.Register(&Envelope{ID: "pending-1", Type: "test"})
		tracker.Register(&Envelope{ID: "pending-2", Type: "test"})
		tracker.Register(&Envelope{ID: "processing-1", Type: "test"})
		tracker.MarkProcessing("processing-1")
		tracker.Register(&Envelope{ID: "completed-1", Type: "test"})
		tracker.MarkCompleted("completed-1")

		pending := tracker.ListByStatus(StatusPending)
		if len(pending) != 2 {
			t.Errorf("got %d pending jobs, want 2", len(pending))
		}

		processing := tracker.ListByStatus(StatusProcessing)
		if len(processing) != 1 {
			t.Errorf("got %d processing jobs, want 1", len(processing))
		}

		completed := tracker.ListByStatus(StatusCompleted)
		if len(completed) != 1 {
			t.Errorf("got %d completed jobs, want 1", len(completed))
		}
	})

	t.Run("returns empty list for status with no jobs", func(t *testing.T) {
		tracker := NewJobTracker()
		tracker.Register(&Envelope{ID: "job-1", Type: "test"})

		failed := tracker.ListByStatus(StatusFailed)

		if len(failed) != 0 {
			t.Errorf("got %d failed jobs, want 0", len(failed))
		}
	})
}

func TestJobTracker_Concurrency(t *testing.T) {
	t.Run("handles concurrent operations safely", func(t *testing.T) {
		tracker := NewJobTracker()
		var wg sync.WaitGroup

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				jobID := JobID("concurrent-" + string(rune('a'+id)))
				tracker.Register(&Envelope{ID: jobID, Type: "test"})
				tracker.MarkProcessing(jobID)
				if id%2 == 0 {
					tracker.MarkCompleted(jobID)
				} else {
					tracker.MarkFailed(jobID, errors.New("test error"))
				}
			}(i)
		}

		wg.Wait()

		jobs := tracker.List()
		if len(jobs) != 10 {
			t.Errorf("got %d jobs, want 10", len(jobs))
		}

		completed := tracker.ListByStatus(StatusCompleted)
		failed := tracker.ListByStatus(StatusFailed)

		if len(completed)+len(failed) != 10 {
			t.Errorf("got %d completed+failed, want 10", len(completed)+len(failed))
		}
	})

	t.Run("concurrent reads and writes", func(t *testing.T) {
		tracker := NewJobTracker()
		tracker.Register(&Envelope{ID: "read-write-test", Type: "test"})

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				tracker.Get("read-write-test")
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				tracker.List()
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				tracker.ListByStatus(StatusPending)
			}()
		}

		wg.Wait()
	})
}

func TestJobTracker_StatusTransitions(t *testing.T) {
	t.Run("tracks complete job lifecycle", func(t *testing.T) {
		tracker := NewJobTracker()
		envelope := &Envelope{
			ID:   "lifecycle-test",
			Type: "test",
		}

		tracker.Register(envelope)
		info, _ := tracker.Get("lifecycle-test")
		if info.Status != StatusPending {
			t.Errorf("initial status: got %q, want %q", info.Status, StatusPending)
		}
		if info.StartedAt != nil {
			t.Error("expected StartedAt to be nil initially")
		}
		if info.FinishedAt != nil {
			t.Error("expected FinishedAt to be nil initially")
		}

		tracker.MarkProcessing("lifecycle-test")
		info, _ = tracker.Get("lifecycle-test")
		if info.Status != StatusProcessing {
			t.Errorf("processing status: got %q, want %q", info.Status, StatusProcessing)
		}
		if info.StartedAt == nil {
			t.Error("expected StartedAt to be set after MarkProcessing")
		}
		if info.FinishedAt != nil {
			t.Error("expected FinishedAt to be nil while processing")
		}

		tracker.MarkCompleted("lifecycle-test")
		info, _ = tracker.Get("lifecycle-test")
		if info.Status != StatusCompleted {
			t.Errorf("completed status: got %q, want %q", info.Status, StatusCompleted)
		}
		if info.StartedAt == nil {
			t.Error("expected StartedAt to remain set")
		}
		if info.FinishedAt == nil {
			t.Error("expected FinishedAt to be set after MarkCompleted")
		}
		if info.Error != "" {
			t.Errorf("expected no error, got %q", info.Error)
		}
	})
}
