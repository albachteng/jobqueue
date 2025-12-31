package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/api"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/tracking"
)

func newTestServer() *api.Server {
	registry := jobs.NewRegistry()

	registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	registry.MustRegister("test", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	return api.NewServer(registry, slog.Default())
}

func TestHandleRoot(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	api.HandleRoot(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	want := "Hello, World!\n"
	if got := w.Body.String(); got != want {
		t.Errorf("got body %q, want %q", got, want)
	}
}

func TestHandleHealth(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	api.HandleHealth(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
	}

	want := "OK\n"
	if got := w.Body.String(); got != want {
		t.Errorf("got body %q, want %q", got, want)
	}
}

func TestHTTP_EnqueueJob(t *testing.T) {
	srv := newTestServer()

	t.Run("creates envelope with job type and payload", func(t *testing.T) {
		reqBody := api.EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message": "hello"}`),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response api.EnqueueResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response.JobID == "" {
			t.Error("expected non-empty job ID")
		}

		if response.Status != "enqueued" {
			t.Errorf("got status %q, want %q", response.Status, "enqueued")
		}
	})

	t.Run("returns 400 for unknown job type", func(t *testing.T) {
		reqBody := api.EnqueueRequest{
			Type:    "unknown",
			Payload: json.RawMessage(`{}`),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("got status %d, want %d for unknown job type", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("returns 400 for invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("got status %d, want %d for invalid JSON", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("accepts complex payload structures", func(t *testing.T) {
		payload := map[string]interface{}{
			"to":          "test@example.com",
			"subject":     "Test",
			"body":        "Hello World",
			"attachments": []string{"file1.pdf", "file2.doc"},
		}
		payloadJSON, _ := json.Marshal(payload)

		reqBody := api.EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(payloadJSON),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}
	})

	t.Run("accepts scheduled_at for future jobs", func(t *testing.T) {
		futureTime := time.Now().Add(1 * time.Hour)
		reqBody := api.EnqueueRequest{
			Type:        "echo",
			Payload:     json.RawMessage(`{"message":"scheduled"}`),
			ScheduledAt: &futureTime,
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response api.EnqueueResponse
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response.JobID == "" {
			t.Error("expected non-empty job ID")
		}

		if response.Status != "enqueued" {
			t.Errorf("got status %q, want %q", response.Status, "enqueued")
		}
	})
}

func TestHTTP_GetJob(t *testing.T) {
	t.Run("returns job info by ID", func(t *testing.T) {
		srv := newTestServer()

		reqBody := api.EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message":"test"}`),
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.HandleEnqueue(enqW, enqReq)

		var enqResp api.EnqueueResponse
		json.NewDecoder(enqW.Body).Decode(&enqResp)

		getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+string(enqResp.JobID), nil)
		getReq.SetPathValue("id", string(enqResp.JobID))
		getW := httptest.NewRecorder()
		srv.HandleGetJob(getW, getReq)

		if getW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", getW.Code, http.StatusOK)
		}

		var jobInfo tracking.JobInfo
		if err := json.NewDecoder(getW.Body).Decode(&jobInfo); err != nil {
			t.Fatalf("failed to decode job info: %v", err)
		}

		if jobInfo.Envelope.ID != enqResp.JobID {
			t.Errorf("got job ID %q, want %q", jobInfo.Envelope.ID, enqResp.JobID)
		}

		if jobInfo.Status != tracking.StatusPending {
			t.Errorf("got status %q, want %q", jobInfo.Status, tracking.StatusPending)
		}
	})

	t.Run("returns 404 for unknown job ID", func(t *testing.T) {
		srv := newTestServer()

		req := httptest.NewRequest(http.MethodGet, "/jobs/unknown-id", nil)
		req.SetPathValue("id", "unknown-id")
		w := httptest.NewRecorder()

		srv.HandleGetJob(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("got status %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestHTTP_ListJobs(t *testing.T) {
	t.Run("returns all jobs", func(t *testing.T) {
		srv := newTestServer()

		for i := 0; i < 3; i++ {
			reqBody := api.EnqueueRequest{
				Type:    "echo",
				Payload: json.RawMessage(`{}`),
			}
			body, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.HandleEnqueue(w, req)
		}

		listReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		listW := httptest.NewRecorder()
		srv.HandleListJobs(listW, listReq)

		if listW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", listW.Code, http.StatusOK)
		}

		var jobList []*tracking.JobInfo
		if err := json.NewDecoder(listW.Body).Decode(&jobList); err != nil {
			t.Fatalf("failed to decode job list: %v", err)
		}

		if len(jobList) != 3 {
			t.Errorf("got %d jobs, want 3", len(jobList))
		}
	})

	t.Run("filters jobs by status", func(t *testing.T) {
		srv := newTestServer()

		reqBody := api.EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{}`),
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.HandleEnqueue(w, req)

		listReq := httptest.NewRequest(http.MethodGet, "/jobs?status=pending", nil)
		listW := httptest.NewRecorder()
		srv.HandleListJobs(listW, listReq)

		if listW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", listW.Code, http.StatusOK)
		}

		var jobList []*tracking.JobInfo
		json.NewDecoder(listW.Body).Decode(&jobList)

		if len(jobList) != 1 {
			t.Errorf("got %d pending jobs, want 1", len(jobList))
		}

		if len(jobList) > 0 && jobList[0].Status != tracking.StatusPending {
			t.Errorf("got status %q, want %q", jobList[0].Status, tracking.StatusPending)
		}
	})

	t.Run("returns empty list when no jobs", func(t *testing.T) {
		srv := newTestServer()

		req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		w := httptest.NewRecorder()
		srv.HandleListJobs(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
		}

		var jobList []*tracking.JobInfo
		json.NewDecoder(w.Body).Decode(&jobList)

		if len(jobList) != 0 {
			t.Errorf("got %d jobs, want 0", len(jobList))
		}
	})
}

func TestHTTP_EndToEnd(t *testing.T) {
	t.Run("enqueue and get job preserves data", func(t *testing.T) {
		srv := newTestServer()

		originalPayload := json.RawMessage(`{"message":"hello world","priority":5}`)
		reqBody := api.EnqueueRequest{
			Type:    "echo",
			Payload: originalPayload,
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.HandleEnqueue(enqW, enqReq)

		var enqResp api.EnqueueResponse
		json.NewDecoder(enqW.Body).Decode(&enqResp)

		getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+string(enqResp.JobID), nil)
		getReq.SetPathValue("id", string(enqResp.JobID))
		getW := httptest.NewRecorder()
		srv.HandleGetJob(getW, getReq)

		var jobInfo tracking.JobInfo
		json.NewDecoder(getW.Body).Decode(&jobInfo)

		if jobInfo.Envelope.ID != enqResp.JobID {
			t.Errorf("job ID mismatch: enqueued %s, got %s", enqResp.JobID, jobInfo.Envelope.ID)
		}

		if string(jobInfo.Envelope.Payload) != string(originalPayload) {
			t.Errorf("payload mismatch:\nenqueued: %s\ngot: %s", originalPayload, jobInfo.Envelope.Payload)
		}
	})

	t.Run("enqueues multiple job types", func(t *testing.T) {
		srv := newTestServer()

		srv.Registry.MustRegister("email", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		testJobs := []struct {
			typ     jobs.JobType
			payload string
		}{
			{"echo", `{"msg":"first"}`},
			{"email", `{"to":"test@example.com"}`},
			{"echo", `{"msg":"third"}`},
		}

		for _, job := range testJobs {
			reqBody := api.EnqueueRequest{
				Type:    job.typ,
				Payload: json.RawMessage(job.payload),
			}
			body, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.HandleEnqueue(w, req)

			if w.Code != http.StatusCreated {
				t.Fatalf("enqueue failed with status %d", w.Code)
			}
		}

		listReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		listW := httptest.NewRecorder()
		srv.HandleListJobs(listW, listReq)

		var jobList []*tracking.JobInfo
		json.NewDecoder(listW.Body).Decode(&jobList)

		if len(jobList) != 3 {
			t.Fatalf("got %d jobs, want 3", len(jobList))
		}
	})

	t.Run("integration with dispatcher and handlers", func(t *testing.T) {
		registry := jobs.NewRegistry()

		var processedCount int
		var processedIDs []jobs.JobID

		handler := jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			processedCount++
			processedIDs = append(processedIDs, env.ID)
			return nil
		})

		registry.MustRegister("integration", handler)

		srv := api.NewServer(registry, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Note: This test can't actually start the dispatcher without it being implemented
		// For now, we just test the HTTP layer
		_ = ctx

		for i := 0; i < 3; i++ {
			reqBody := api.EnqueueRequest{
				Type:    "integration",
				Payload: json.RawMessage(`{}`),
			}
			body, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.HandleEnqueue(w, req)

			if w.Code != http.StatusCreated {
				t.Fatalf("enqueue failed")
			}
		}

		time.Sleep(50 * time.Millisecond)

		// For now, just verify jobs are in queue
		// Once dispatcher is implemented, verify processedCount == 3
		_ = processedCount
		_ = processedIDs
	})
}

func TestHTTP_ContextPropagation(t *testing.T) {
	t.Run("handleEnqueue uses request context", func(t *testing.T) {
		srv := newTestServer()

		var capturedCtx context.Context
		mockQueue := &contextCapturingQueue{
			onEnqueue: func(ctx context.Context, env *jobs.Envelope) error {
				capturedCtx = ctx
				return nil
			},
		}
		srv.Queue = mockQueue

		reqBody := api.EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{}`),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.HandleEnqueue(w, req)

		if capturedCtx == nil {
			t.Fatal("context was not passed to queue.Enqueue")
		}

		if capturedCtx != req.Context() {
			t.Error("handleEnqueue should use request context, not a different context")
		}
	})

}

type contextCapturingQueue struct {
	onEnqueue func(ctx context.Context, env *jobs.Envelope) error
	onDequeue func(ctx context.Context) (*jobs.Envelope, error)
}

func (m *contextCapturingQueue) Enqueue(ctx context.Context, item *jobs.Envelope) error {
	if m.onEnqueue != nil {
		return m.onEnqueue(ctx, item)
	}
	return nil
}

func (m *contextCapturingQueue) Dequeue(ctx context.Context) (*jobs.Envelope, error) {
	if m.onDequeue != nil {
		return m.onDequeue(ctx)
	}
	return nil, queue.ErrEmptyQueue
}
