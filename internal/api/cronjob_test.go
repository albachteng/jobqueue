package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

func TestHandleCreateCronJob(t *testing.T) {
	dbPath := t.TempDir() + "/create_cronjob_test.db"
	q, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	srv := NewTestServer(q)

	t.Run("create valid cron job", func(t *testing.T) {
		reqBody := CreateCronJobRequest{
			Name:       "Test Cron",
			CronExpr:   "*/5 * * * *",
			JobType:    "echo",
			Payload:    json.RawMessage(`{"message": "test"}`),
			Priority:   5,
			MaxRetries: 3,
			Timeout:    "30s",
			Enabled:    true,
		}

		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/cron-jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()

		srv.HandleCreateCronJob(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("expected status 201, got %d: %s", w.Code, w.Body.String())
		}

		var resp CreateCronJobResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if resp.CronJobID == "" {
			t.Error("expected cron job ID to be set")
		}

		cronJob, exists := q.GetCronJob(context.Background(), cron.CronJobID(resp.CronJobID))
		if !exists {
			t.Error("expected cron job to exist in database")
		}

		if cronJob.Name != reqBody.Name {
			t.Errorf("expected name %q, got %q", reqBody.Name, cronJob.Name)
		}
	})

	t.Run("reject invalid cron expression", func(t *testing.T) {
		reqBody := CreateCronJobRequest{
			Name:     "Invalid Cron",
			CronExpr: "invalid expression",
			JobType:  "echo",
			Payload:  json.RawMessage(`{"message": "test"}`),
			Enabled:  true,
		}

		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/cron-jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()

		srv.HandleCreateCronJob(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("reject empty job type", func(t *testing.T) {
		reqBody := CreateCronJobRequest{
			Name:     "No Type",
			CronExpr: "*/5 * * * *",
			JobType:  "",
			Payload:  json.RawMessage(`{"message": "test"}`),
			Enabled:  true,
		}

		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/cron-jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()

		srv.HandleCreateCronJob(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})

	t.Run("parse timeout duration", func(t *testing.T) {
		reqBody := CreateCronJobRequest{
			Name:       "Timeout Test",
			CronExpr:   "*/5 * * * *",
			JobType:    "echo",
			Payload:    json.RawMessage(`{"message": "test"}`),
			Timeout:    "2m30s",
			Enabled:    true,
		}

		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/cron-jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()

		srv.HandleCreateCronJob(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("expected status 201, got %d: %s", w.Code, w.Body.String())
		}

		var resp CreateCronJobResponse
		json.NewDecoder(w.Body).Decode(&resp)

		cronJob, _ := q.GetCronJob(context.Background(), cron.CronJobID(resp.CronJobID))
		if cronJob.Timeout != 150*time.Second {
			t.Errorf("expected timeout 150s, got %v", cronJob.Timeout)
		}
	})

	t.Run("reject invalid timeout format", func(t *testing.T) {
		reqBody := CreateCronJobRequest{
			Name:     "Invalid Timeout",
			CronExpr: "*/5 * * * *",
			JobType:  "echo",
			Payload:  json.RawMessage(`{"message": "test"}`),
			Timeout:  "invalid",
			Enabled:  true,
		}

		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest("POST", "/cron-jobs", bytes.NewReader(body))
		w := httptest.NewRecorder()

		srv.HandleCreateCronJob(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", w.Code)
		}
	})
}

func TestHandleListCronJobs(t *testing.T) {
	dbPath := t.TempDir() + "/list_cronjobs_test.db"
	q, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	srv := NewTestServer(q)
	ctx := context.Background()

	payload := json.RawMessage(`{"message": "test"}`)
	cronJob1 := &cron.CronJob{
		ID:        "cron-1",
		Name:      "First Cron",
		CronExpr:  "*/5 * * * *",
		JobType:   "echo",
		Payload:   payload,
		Enabled:   true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	cronJob2 := &cron.CronJob{
		ID:        "cron-2",
		Name:      "Second Cron",
		CronExpr:  "0 0 * * *",
		JobType:   "echo",
		Payload:   payload,
		Enabled:   false,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	q.CreateCronJob(ctx, cronJob1)
	q.CreateCronJob(ctx, cronJob2)

	req := httptest.NewRequest("GET", "/cron-jobs", nil)
	w := httptest.NewRecorder()

	srv.HandleListCronJobs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var cronJobs []*cron.CronJob
	if err := json.NewDecoder(w.Body).Decode(&cronJobs); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if len(cronJobs) != 2 {
		t.Errorf("expected 2 cron jobs, got %d", len(cronJobs))
	}
}

func TestHandleGetCronJob(t *testing.T) {
	dbPath := t.TempDir() + "/get_cronjob_test.db"
	q, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	srv := NewTestServer(q)
	ctx := context.Background()

	payload := json.RawMessage(`{"message": "test"}`)
	cronJob := &cron.CronJob{
		ID:        "cron-get",
		Name:      "Get Test",
		CronExpr:  "*/5 * * * *",
		JobType:   "echo",
		Payload:   payload,
		Enabled:   true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	q.CreateCronJob(ctx, cronJob)

	t.Run("get existing cron job", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/cron-jobs/cron-get", nil)
		req.SetPathValue("id", "cron-get")
		w := httptest.NewRecorder()

		srv.HandleGetCronJob(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		var retrieved cron.CronJob
		if err := json.NewDecoder(w.Body).Decode(&retrieved); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}

		if retrieved.ID != cronJob.ID {
			t.Errorf("expected ID %q, got %q", cronJob.ID, retrieved.ID)
		}
	})

	t.Run("get non-existent cron job", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/cron-jobs/nonexistent", nil)
		req.SetPathValue("id", "nonexistent")
		w := httptest.NewRecorder()

		srv.HandleGetCronJob(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})
}

func TestHandleUpdateCronJob(t *testing.T) {
	dbPath := t.TempDir() + "/update_cronjob_test.db"
	q, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	srv := NewTestServer(q)
	ctx := context.Background()

	payload := json.RawMessage(`{"message": "test"}`)
	cronJob := &cron.CronJob{
		ID:        "cron-update",
		Name:      "Update Test",
		CronExpr:  "*/5 * * * *",
		JobType:   "echo",
		Payload:   payload,
		Enabled:   true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	q.CreateCronJob(ctx, cronJob)

	t.Run("update cron job", func(t *testing.T) {
		updateReq := UpdateCronJobRequest{
			Name:       "Updated Name",
			CronExpr:   "*/10 * * * *",
			Priority:   10,
			MaxRetries: 5,
			Enabled:    false,
		}

		body, _ := json.Marshal(updateReq)
		req := httptest.NewRequest("PUT", "/cron-jobs/cron-update", bytes.NewReader(body))
		req.SetPathValue("id", "cron-update")
		w := httptest.NewRecorder()

		srv.HandleUpdateCronJob(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d: %s", w.Code, w.Body.String())
		}

		updated, exists := q.GetCronJob(ctx, "cron-update")
		if !exists {
			t.Fatal("expected cron job to exist")
		}

		if updated.Name != "Updated Name" {
			t.Errorf("expected name 'Updated Name', got %q", updated.Name)
		}
		if updated.CronExpr != "*/10 * * * *" {
			t.Errorf("expected cron expr '*/10 * * * *', got %q", updated.CronExpr)
		}
		if updated.Enabled {
			t.Error("expected enabled to be false")
		}
	})

	t.Run("update non-existent cron job", func(t *testing.T) {
		updateReq := UpdateCronJobRequest{
			Name: "Doesn't Matter",
		}

		body, _ := json.Marshal(updateReq)
		req := httptest.NewRequest("PUT", "/cron-jobs/nonexistent", bytes.NewReader(body))
		req.SetPathValue("id", "nonexistent")
		w := httptest.NewRecorder()

		srv.HandleUpdateCronJob(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})
}

func TestHandleDeleteCronJob(t *testing.T) {
	dbPath := t.TempDir() + "/delete_cronjob_test.db"
	q, err := queue.NewSQLiteQueue(dbPath)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	defer q.Close()

	srv := NewTestServer(q)
	ctx := context.Background()

	payload := json.RawMessage(`{"message": "test"}`)
	cronJob := &cron.CronJob{
		ID:        "cron-delete",
		Name:      "Delete Test",
		CronExpr:  "*/5 * * * *",
		JobType:   "echo",
		Payload:   payload,
		Enabled:   true,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	q.CreateCronJob(ctx, cronJob)

	t.Run("delete existing cron job", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/cron-jobs/cron-delete", nil)
		req.SetPathValue("id", "cron-delete")
		w := httptest.NewRecorder()

		srv.HandleDeleteCronJob(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", w.Code)
		}

		_, exists := q.GetCronJob(ctx, "cron-delete")
		if exists {
			t.Error("expected cron job to be deleted")
		}
	})

	t.Run("delete non-existent cron job", func(t *testing.T) {
		req := httptest.NewRequest("DELETE", "/cron-jobs/nonexistent", nil)
		req.SetPathValue("id", "nonexistent")
		w := httptest.NewRecorder()

		srv.HandleDeleteCronJob(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status 404, got %d", w.Code)
		}
	})
}
