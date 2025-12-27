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

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

func newTestServer() *server {
	registry := jobs.NewRegistry()

	registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	registry.MustRegister("test", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	return newServer(registry, slog.Default())
}

func TestHandleRoot(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	handleRoot(w, req)

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

	handleHealth(w, req)

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
		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message": "hello"}`),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.handleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}

		var response EnqueueResponse
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
		reqBody := EnqueueRequest{
			Type:    "unknown",
			Payload: json.RawMessage(`{}`),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.handleEnqueue(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("got status %d, want %d for unknown job type", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("returns 400 for invalid JSON", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader([]byte("invalid json")))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.handleEnqueue(w, req)

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

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(payloadJSON),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.handleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
		}
	})
}

func TestHTTP_GetJob(t *testing.T) {
	t.Run("returns job info by ID", func(t *testing.T) {
		srv := newTestServer()

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message":"test"}`),
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.handleEnqueue(enqW, enqReq)

		var enqResp EnqueueResponse
		json.NewDecoder(enqW.Body).Decode(&enqResp)

		getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+string(enqResp.JobID), nil)
		getReq.SetPathValue("id", string(enqResp.JobID))
		getW := httptest.NewRecorder()
		srv.handleGetJob(getW, getReq)

		if getW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", getW.Code, http.StatusOK)
		}

		var jobInfo jobs.JobInfo
		if err := json.NewDecoder(getW.Body).Decode(&jobInfo); err != nil {
			t.Fatalf("failed to decode job info: %v", err)
		}

		if jobInfo.Envelope.ID != enqResp.JobID {
			t.Errorf("got job ID %q, want %q", jobInfo.Envelope.ID, enqResp.JobID)
		}

		if jobInfo.Status != jobs.StatusPending {
			t.Errorf("got status %q, want %q", jobInfo.Status, jobs.StatusPending)
		}
	})

	t.Run("returns 404 for unknown job ID", func(t *testing.T) {
		srv := newTestServer()

		req := httptest.NewRequest(http.MethodGet, "/jobs/unknown-id", nil)
		req.SetPathValue("id", "unknown-id")
		w := httptest.NewRecorder()

		srv.handleGetJob(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("got status %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestHTTP_ListJobs(t *testing.T) {
	t.Run("returns all jobs", func(t *testing.T) {
		srv := newTestServer()

		for i := 0; i < 3; i++ {
			reqBody := EnqueueRequest{
				Type:    "echo",
				Payload: json.RawMessage(`{}`),
			}
			body, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			srv.handleEnqueue(w, req)
		}

		listReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		listW := httptest.NewRecorder()
		srv.handleListJobs(listW, listReq)

		if listW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", listW.Code, http.StatusOK)
		}

		var jobList []*jobs.JobInfo
		if err := json.NewDecoder(listW.Body).Decode(&jobList); err != nil {
			t.Fatalf("failed to decode job list: %v", err)
		}

		if len(jobList) != 3 {
			t.Errorf("got %d jobs, want 3", len(jobList))
		}
	})

	t.Run("filters jobs by status", func(t *testing.T) {
		srv := newTestServer()

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{}`),
		}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		srv.handleEnqueue(w, req)

		listReq := httptest.NewRequest(http.MethodGet, "/jobs?status=pending", nil)
		listW := httptest.NewRecorder()
		srv.handleListJobs(listW, listReq)

		if listW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", listW.Code, http.StatusOK)
		}

		var jobList []*jobs.JobInfo
		json.NewDecoder(listW.Body).Decode(&jobList)

		if len(jobList) != 1 {
			t.Errorf("got %d pending jobs, want 1", len(jobList))
		}

		if len(jobList) > 0 && jobList[0].Status != jobs.StatusPending {
			t.Errorf("got status %q, want %q", jobList[0].Status, jobs.StatusPending)
		}
	})

	t.Run("returns empty list when no jobs", func(t *testing.T) {
		srv := newTestServer()

		req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		w := httptest.NewRecorder()
		srv.handleListJobs(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", w.Code, http.StatusOK)
		}

		var jobList []*jobs.JobInfo
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
		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: originalPayload,
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.handleEnqueue(enqW, enqReq)

		var enqResp EnqueueResponse
		json.NewDecoder(enqW.Body).Decode(&enqResp)

		getReq := httptest.NewRequest(http.MethodGet, "/jobs/"+string(enqResp.JobID), nil)
		getReq.SetPathValue("id", string(enqResp.JobID))
		getW := httptest.NewRecorder()
		srv.handleGetJob(getW, getReq)

		var jobInfo jobs.JobInfo
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

		srv.registry.MustRegister("email", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
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
			reqBody := EnqueueRequest{
				Type:    job.typ,
				Payload: json.RawMessage(job.payload),
			}
			body, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.handleEnqueue(w, req)

			if w.Code != http.StatusCreated {
				t.Fatalf("enqueue failed with status %d", w.Code)
			}
		}

		listReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		listW := httptest.NewRecorder()
		srv.handleListJobs(listW, listReq)

		var jobList []*jobs.JobInfo
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

		srv := newServer(registry, slog.Default())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Note: This test can't actually start the dispatcher without it being implemented
		// For now, we just test the HTTP layer
		_ = ctx

		for i := 0; i < 3; i++ {
			reqBody := EnqueueRequest{
				Type:    "integration",
				Payload: json.RawMessage(`{}`),
			}
			body, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			srv.handleEnqueue(w, req)

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
		srv.queue = mockQueue

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{}`),
		}
		body, _ := json.Marshal(reqBody)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.handleEnqueue(w, req)

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
