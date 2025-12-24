package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestServer() *server {
	return newServer()
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

func TestHandleEnqueue(t *testing.T) {
	srv := newTestServer()

	jobPayload := map[string]string{"message": "test job"}
	body, _ := json.Marshal(jobPayload)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	srv.handleEnqueue(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("got status %d, want %d", w.Code, http.StatusCreated)
	}

	var response map[string]string
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["status"] != "enqueued" {
		t.Errorf("got status %q, want %q", response["status"], "enqueued")
	}
}

func TestHandleDequeue(t *testing.T) {
	srv := newTestServer()

	jobPayload := map[string]string{"message": "test job"}
	body, _ := json.Marshal(jobPayload)

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

	var response map[string]string
	if err := json.NewDecoder(deqW.Body).Decode(&response); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if response["message"] != "test job" {
		t.Errorf("got message %q, want %q", response["message"], "test job")
	}
}

func TestHandleDequeueEmpty(t *testing.T) {
	srv := newTestServer()

	req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
	w := httptest.NewRecorder()

	srv.handleDequeue(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("got status %d, want %d", w.Code, http.StatusNoContent)
	}
}

func TestEnqueueDequeueRoundTrip(t *testing.T) {
	srv := newTestServer()

	jobs := []string{"job1", "job2", "job3"}

	for _, msg := range jobs {
		jobPayload := map[string]string{"message": msg}
		body, _ := json.Marshal(jobPayload)

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		srv.handleEnqueue(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("enqueue failed with status %d", w.Code)
		}
	}

	for _, want := range jobs {
		req := httptest.NewRequest(http.MethodGet, "/jobs", nil)
		w := httptest.NewRecorder()

		srv.handleDequeue(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("dequeue failed with status %d", w.Code)
		}

		var response map[string]string
		if err := json.NewDecoder(w.Body).Decode(&response); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if response["message"] != want {
			t.Errorf("got message %q, want %q (FIFO order broken)", response["message"], want)
		}
	}
}
