package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

func TestHandleEnqueue_MaxRetries(t *testing.T) {
	t.Run("accepts max_retries in request", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		dbPath := t.TempDir() + "/maxretries_test.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		reqBody := EnqueueRequest{
			Type:       "echo",
			Payload:    json.RawMessage(`{"message":"test"}`),
			MaxRetries: 5,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response EnqueueResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		// Verify the job was created with correct max_retries
		record, exists := persQueue.GetJob(context.Background(), response.JobID)
		if !exists {
			t.Fatal("job not found in queue")
		}

		if record.MaxRetries != 5 {
			t.Errorf("expected MaxRetries to be 5, got %d", record.MaxRetries)
		}
	})

	t.Run("defaults max_retries to 0 when not specified", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		dbPath := t.TempDir() + "/maxretries_default.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message":"test"}`),
			// MaxRetries not specified
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response EnqueueResponse
		json.NewDecoder(w.Body).Decode(&response)

		// Verify the job has default max_retries of 0
		record, exists := persQueue.GetJob(context.Background(), response.JobID)
		if !exists {
			t.Fatal("job not found in queue")
		}

		if record.MaxRetries != 0 {
			t.Errorf("expected default MaxRetries to be 0, got %d", record.MaxRetries)
		}
	})

	t.Run("accepts zero max_retries explicitly", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		dbPath := t.TempDir() + "/maxretries_zero.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		reqBody := EnqueueRequest{
			Type:       "echo",
			Payload:    json.RawMessage(`{"message":"test"}`),
			MaxRetries: 0, // Explicitly set to 0
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response EnqueueResponse
		json.NewDecoder(w.Body).Decode(&response)

		record, exists := persQueue.GetJob(context.Background(), response.JobID)
		if !exists {
			t.Fatal("job not found in queue")
		}

		if record.MaxRetries != 0 {
			t.Errorf("expected MaxRetries to be 0, got %d", record.MaxRetries)
		}
	})

	t.Run("rejects negative max_retries", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		srv := NewServer(registry, slog.Default())

		reqBody := EnqueueRequest{
			Type:       "echo",
			Payload:    json.RawMessage(`{"message":"test"}`),
			MaxRetries: -1, // Negative value
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for negative max_retries, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("accepts large max_retries values", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		dbPath := t.TempDir() + "/maxretries_large.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		reqBody := EnqueueRequest{
			Type:       "echo",
			Payload:    json.RawMessage(`{"message":"test"}`),
			MaxRetries: 100,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response EnqueueResponse
		json.NewDecoder(w.Body).Decode(&response)

		record, exists := persQueue.GetJob(context.Background(), response.JobID)
		if !exists {
			t.Fatal("job not found in queue")
		}

		if record.MaxRetries != 100 {
			t.Errorf("expected MaxRetries to be 100, got %d", record.MaxRetries)
		}
	})
}
