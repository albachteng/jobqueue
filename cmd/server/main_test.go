package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/jobs"
)

func newTestServer() *server {
	registry := jobs.NewRegistry()

	registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	registry.MustRegister("test", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	return newServer(registry)
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

func TestHTTP_DequeueJob(t *testing.T) {
	t.Run("returns full envelope", func(t *testing.T) {
		srv := newTestServer()

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"message": "test"}`),
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.handleEnqueue(enqW, enqReq)

		deqReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		deqW := httptest.NewRecorder()
		srv.handleDequeue(deqW, deqReq)

		if deqW.Code != http.StatusOK {
			t.Errorf("got status %d, want %d", deqW.Code, http.StatusOK)
		}

		var envelope jobs.Envelope
		if err := json.NewDecoder(deqW.Body).Decode(&envelope); err != nil {
			t.Fatalf("failed to decode envelope: %v", err)
		}

		if envelope.ID == "" {
			t.Error("expected non-empty envelope ID")
		}

		if envelope.Type != "echo" {
			t.Errorf("got type %q, want %q", envelope.Type, "echo")
		}

		if len(envelope.Payload) == 0 {
			t.Error("expected non-empty payload")
		}

		if envelope.Status != "pending" {
			t.Errorf("got status %q, want %q", envelope.Status, "pending")
		}
	})

	t.Run("returns 204 for empty queue", func(t *testing.T) {
		srv := newTestServer()

		req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		w := httptest.NewRecorder()

		srv.handleDequeue(w, req)

		if w.Code != http.StatusNoContent {
			t.Errorf("got status %d, want %d", w.Code, http.StatusNoContent)
		}
	})

	t.Run("includes all envelope fields in response", func(t *testing.T) {
		srv := newTestServer()

		reqBody := EnqueueRequest{
			Type:    "echo",
			Payload: json.RawMessage(`{"data": "value"}`),
		}
		body, _ := json.Marshal(reqBody)

		enqReq := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		enqReq.Header.Set("Content-Type", "application/json")
		enqW := httptest.NewRecorder()
		srv.handleEnqueue(enqW, enqReq)

		deqReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		deqW := httptest.NewRecorder()
		srv.handleDequeue(deqW, deqReq)

		var envelope jobs.Envelope
		json.NewDecoder(deqW.Body).Decode(&envelope)

		if envelope.ID == "" {
			t.Error("missing ID")
		}
		if envelope.Type == "" {
			t.Error("missing Type")
		}
		if len(envelope.Payload) == 0 {
			t.Error("missing Payload")
		}
		if envelope.CreatedAt.IsZero() {
			t.Error("missing CreatedAt")
		}
		if envelope.Status == "" {
			t.Error("missing Status")
		}
	})
}

func TestHTTP_EndToEnd(t *testing.T) {
	t.Run("enqueue and dequeue preserves data", func(t *testing.T) {
		srv := newTestServer()

		originalPayload := json.RawMessage(`{"message": "hello world", "priority": 5}`)
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

		deqReq := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		deqW := httptest.NewRecorder()
		srv.handleDequeue(deqW, deqReq)

		var envelope jobs.Envelope
		json.NewDecoder(deqW.Body).Decode(&envelope)

		if envelope.ID != enqResp.JobID {
			t.Errorf("job ID mismatch: enqueued %s, dequeued %s", enqResp.JobID, envelope.ID)
		}

		if string(envelope.Payload) != string(originalPayload) {
			t.Errorf("payload mismatch:\nenqueued: %s\ndequeued: %s", originalPayload, envelope.Payload)
		}
	})

	t.Run("multiple job types maintain order", func(t *testing.T) {
		srv := newTestServer()

		srv.registry.MustRegister("email", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
			return nil
		}))

		jobs := []struct {
			typ     jobs.JobType
			payload string
		}{
			{"echo", `{"msg": "first"}`},
			{"email", `{"to": "test@example.com"}`},
			{"echo", `{"msg": "third"}`},
		}

		for _, job := range jobs {
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

		for i, want := range jobs {
			req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
			w := httptest.NewRecorder()

			srv.handleDequeue(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("dequeue %d failed with status %d", i, w.Code)
			}

			var envelope jobs.Envelope
			json.NewDecoder(w.Body).Decode(&envelope)

			if envelope.Type != want.typ {
				t.Errorf("job %d: got type %s, want %s (FIFO order broken)", i, envelope.Type, want.typ)
			}

			if string(envelope.Payload) != want.payload {
				t.Errorf("job %d: payload mismatch", i)
			}
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

		srv := newServer(registry)

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
