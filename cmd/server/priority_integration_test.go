package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/albachteng/jobqueue/internal/api"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

func TestPriorityQueuing_HTTPtoDB(t *testing.T) {
	tmpDB := t.TempDir() + "/test.db"

	persQueue, err := queue.NewSQLiteQueue(tmpDB)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer persQueue.Close()

	registry := jobs.NewRegistry()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	registry.MustRegister("test", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	srv := api.NewServer(registry, logger)
	srv.Queue = persQueue

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", srv.HandleEnqueue)
	testServer := httptest.NewServer(mux)
	defer testServer.Close()

	testJobs := []struct {
		name     string
		priority int
	}{
		{"job1", 5},
		{"job2", 10},
		{"job3", 1},
		{"job4", -5},  // Negative priority
		{"job5", 0},   // Default priority (omitted from request)
		{"job6", 100}, // High priority
	}

	jobIDs := make(map[string]jobs.JobID)

	for _, tj := range testJobs {
		reqBody := map[string]any{
			"type":    "test",
			"payload": tj.name,
		}
		if tj.priority != 0 {
			reqBody["priority"] = tj.priority
		}
		reqJSON, _ := json.Marshal(reqBody)

		resp, err := http.Post(testServer.URL+"/jobs", "application/json", strings.NewReader(string(reqJSON)))
		if err != nil {
			t.Fatalf("failed to enqueue job %s: %v", tj.name, err)
		}

		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("unexpected status code for job %s: %d", tj.name, resp.StatusCode)
		}

		var enqueueResp api.EnqueueResponse
		if err := json.NewDecoder(resp.Body).Decode(&enqueueResp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		resp.Body.Close()

		jobIDs[tj.name] = enqueueResp.JobID
		t.Logf("Enqueued %s with ID %s and priority %d", tj.name, enqueueResp.JobID, tj.priority)
	}

	// Verify jobs are in database with correct priorities
	// Note that we are not guaranteeing order, only that the priorities are respected
	ctx := context.Background()
	for _, tj := range testJobs {
		jobID := jobIDs[tj.name]
		record, exists := persQueue.GetJob(ctx, jobID)
		if !exists {
			t.Errorf("job %s (%s) not found in database", tj.name, jobID)
			continue
		}

		if record.Priority != tj.priority {
			t.Errorf("job %s: expected priority %d, got %d", tj.name, tj.priority, record.Priority)
		}

		if record.Status != "pending" {
			t.Errorf("job %s: expected status 'pending', got '%s'", tj.name, record.Status)
		}

		t.Logf("Verified %s in database with priority %d", tj.name, record.Priority)
	}

	t.Logf("All %d jobs persisted with correct priorities", len(testJobs))
}
