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

func TestHandleCancelJob(t *testing.T) {
	t.Run("successfully cancels pending job", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		dbPath := t.TempDir() + "/cancel_test.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		// Enqueue a job first
		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message":"test"}`),
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.HandleEnqueue(enqW, enqReq)

		var enqResp EnqueueResponse
		json.NewDecoder(enqW.Body).Decode(&enqResp)
		jobID := enqResp.JobID

		// Cancel the job
		cancelReq := httptest.NewRequest(http.MethodDelete, "/jobs/"+string(jobID), nil)
		cancelReq.SetPathValue("id", string(jobID))
		cancelW := httptest.NewRecorder()

		srv.HandleCancelJob(cancelW, cancelReq)

		if cancelW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", cancelW.Code, http.StatusOK)
		}

		var response map[string]string
		if err := json.NewDecoder(cancelW.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response["status"] != "cancelled" {
			t.Errorf("got status %q, want %q", response["status"], "cancelled")
		}

		if response["job_id"] != string(jobID) {
			t.Errorf("got job_id %q, want %q", response["job_id"], string(jobID))
		}
	})

	t.Run("returns 404 for non-existent job", func(t *testing.T) {
		registry := jobs.NewRegistry()

		dbPath := t.TempDir() + "/cancel_404.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		cancelReq := httptest.NewRequest(http.MethodDelete, "/jobs/non-existent-id", nil)
		cancelReq.SetPathValue("id", "non-existent-id")
		cancelW := httptest.NewRecorder()

		srv.HandleCancelJob(cancelW, cancelReq)

		if cancelW.Code != http.StatusNotFound {
			t.Errorf("got status %d, want %d", cancelW.Code, http.StatusNotFound)
		}
	})

	t.Run("returns 400 when cancelling completed job", func(t *testing.T) {
		registry := jobs.NewRegistry()
		registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		dbPath := t.TempDir() + "/cancel_completed.db"
		persQueue, err := queue.NewSQLiteQueue(dbPath)
		if err != nil {
			t.Fatalf("failed to create queue: %v", err)
		}
		defer persQueue.Close()

		srv := NewServer(registry, slog.Default())
		srv.Queue = persQueue

		// Enqueue and complete a job
		ctx := context.Background()
		env, _ := jobs.NewEnvelope("echo", json.RawMessage(`{}`))
		persQueue.Enqueue(ctx, env)
		persQueue.CompleteJob(ctx, env.ID, 1)

		cancelReq := httptest.NewRequest(http.MethodDelete, "/jobs/"+string(env.ID), nil)
		cancelReq.SetPathValue("id", string(env.ID))
		cancelW := httptest.NewRecorder()

		srv.HandleCancelJob(cancelW, cancelReq)

		if cancelW.Code != http.StatusBadRequest {
			t.Errorf("got status %d, want %d", cancelW.Code, http.StatusBadRequest)
		}
	})

	t.Run("returns 501 when queue doesn't support cancellation", func(t *testing.T) {
		registry := jobs.NewRegistry()
		srv := NewServer(registry, slog.Default())

		// Default in-memory queue doesn't support cancellation

		cancelReq := httptest.NewRequest(http.MethodDelete, "/jobs/some-job-id", nil)
		cancelReq.SetPathValue("id", "some-job-id")
		cancelW := httptest.NewRecorder()

		srv.HandleCancelJob(cancelW, cancelReq)

		if cancelW.Code != http.StatusNotImplemented {
			t.Errorf("got status %d, want %d", cancelW.Code, http.StatusNotImplemented)
		}
	})
}
