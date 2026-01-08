package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/tracking"
)

func generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func (s *Server) HandleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, err := s.Registry.Get(req.Type); err != nil {
		http.Error(w, fmt.Sprintf("unknown job type: %s", req.Type), http.StatusBadRequest)
		return
	}

	envelope, err := jobs.NewEnvelope(req.Type, req.Payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	envelope.Priority = req.Priority
	envelope.ScheduledAt = req.ScheduledAt

	s.Tracker.Register(envelope)

	if err := s.Queue.Enqueue(r.Context(), envelope); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.Logger.Info("job enqueued",
		"job_id", envelope.ID,
		"job_type", envelope.Type,
		"priority", envelope.Priority)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EnqueueResponse{
		JobID:  envelope.ID,
		Status: "enqueued",
	})
}

func (s *Server) HandleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := jobs.JobID(r.PathValue("id"))

	if persQueue, ok := s.Queue.(queue.PersistentQueue); ok {
		record, exists := persQueue.GetJob(r.Context(), jobID)
		if !exists {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(record)
		return
	}

	info, exists := s.Tracker.Get(jobID)
	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(info)
}

func (s *Server) HandleListJobs(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")

	if persQueue, ok := s.Queue.(queue.PersistentQueue); ok {
		var jobList []*queue.JobRecord
		if statusFilter != "" {
			jobList = persQueue.ListJobsByStatus(r.Context(), statusFilter)
		} else {
			allJobs := []*queue.JobRecord{}
			for _, status := range []string{"pending", "processing", "completed", "failed"} {
				allJobs = append(allJobs, persQueue.ListJobsByStatus(r.Context(), status)...)
			}
			jobList = allJobs
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(jobList)
		return
	}

	var jobList []*tracking.JobInfo
	if statusFilter != "" {
		jobList = s.Tracker.ListByStatus(tracking.JobStatus(statusFilter))
	} else {
		jobList = s.Tracker.List()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jobList)
}

func (s *Server) HandleListDLQ(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "DLQ not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	dlqJobs := persQueue.ListDLQJobs(r.Context())

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(dlqJobs)
}

func (s *Server) HandleRequeueDLQ(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "DLQ not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	jobID := jobs.JobID(r.PathValue("id"))

	if err := persQueue.RequeueDLQJob(r.Context(), jobID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.Logger.Info("job requeued from DLQ", "job_id", jobID)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "requeued",
		"job_id": string(jobID),
	})
}

func (s *Server) HandleCancelJob(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "cancellation not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	jobID := jobs.JobID(r.PathValue("id"))

	if err := persQueue.CancelJob(r.Context(), jobID); err != nil {
		if err == queue.ErrJobNotFound {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}
		if err == queue.ErrJobNotCancellable {
			http.Error(w, "job cannot be cancelled in current state", http.StatusBadRequest)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.Logger.Info("job cancelled", "job_id", jobID)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "cancelled",
		"job_id": string(jobID),
	})
}

func (s *Server) HandleCreateCronJob(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "cron jobs not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	var req CreateCronJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.JobType == "" {
		http.Error(w, "job_type is required", http.StatusBadRequest)
		return
	}

	if req.CronExpr == "" {
		http.Error(w, "cron_expr is required", http.StatusBadRequest)
		return
	}

	var timeout time.Duration
	if req.Timeout != "" {
		parsedTimeout, err := time.ParseDuration(req.Timeout)
		if err != nil {
			http.Error(w, "invalid timeout format", http.StatusBadRequest)
			return
		}
		timeout = parsedTimeout
	}

	cronJob := &cron.CronJob{
		ID:         cron.CronJobID(generateID()),
		Name:       req.Name,
		CronExpr:   req.CronExpr,
		JobType:    req.JobType,
		Payload:    req.Payload,
		Priority:   req.Priority,
		MaxRetries: req.MaxRetries,
		Timeout:    timeout,
		Enabled:    req.Enabled,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	if err := persQueue.CreateCronJob(r.Context(), cronJob); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.Logger.Info("cron job created",
		"cron_job_id", cronJob.ID,
		"name", cronJob.Name,
		"cron_expr", cronJob.CronExpr)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(CreateCronJobResponse{
		CronJobID: string(cronJob.ID),
		Status:    "created",
	})
}

func (s *Server) HandleListCronJobs(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "cron jobs not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	cronJobs := persQueue.ListCronJobs(r.Context())

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cronJobs)
}

func (s *Server) HandleGetCronJob(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "cron jobs not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	id := cron.CronJobID(r.PathValue("id"))

	cronJob, exists := persQueue.GetCronJob(r.Context(), id)
	if !exists {
		http.Error(w, "cron job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cronJob)
}

func (s *Server) HandleUpdateCronJob(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "cron jobs not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	id := cron.CronJobID(r.PathValue("id"))

	existing, exists := persQueue.GetCronJob(r.Context(), id)
	if !exists {
		http.Error(w, "cron job not found", http.StatusNotFound)
		return
	}

	var req UpdateCronJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.CronExpr != "" {
		existing.CronExpr = req.CronExpr
	}
	if req.JobType != "" {
		existing.JobType = req.JobType
	}
	if req.Payload != nil {
		existing.Payload = req.Payload
	}
	if req.Priority != 0 {
		existing.Priority = req.Priority
	}
	if req.MaxRetries != 0 {
		existing.MaxRetries = req.MaxRetries
	}
	if req.Timeout != "" {
		parsedTimeout, err := time.ParseDuration(req.Timeout)
		if err != nil {
			http.Error(w, "invalid timeout format", http.StatusBadRequest)
			return
		}
		existing.Timeout = parsedTimeout
	}
	existing.Enabled = req.Enabled
	existing.UpdatedAt = time.Now()

	if err := persQueue.UpdateCronJob(r.Context(), existing); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.Logger.Info("cron job updated", "cron_job_id", id)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":      "updated",
		"cron_job_id": string(id),
	})
}

func (s *Server) HandleDeleteCronJob(w http.ResponseWriter, r *http.Request) {
	persQueue, ok := s.Queue.(queue.PersistentQueue)
	if !ok {
		http.Error(w, "cron jobs not available in non-persistent mode", http.StatusNotImplemented)
		return
	}

	id := cron.CronJobID(r.PathValue("id"))

	if err := persQueue.DeleteCronJob(r.Context(), id); err != nil {
		http.Error(w, "cron job not found", http.StatusNotFound)
		return
	}

	s.Logger.Info("cron job deleted", "cron_job_id", id)

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":      "deleted",
		"cron_job_id": string(id),
	})
}

func HandleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!\n")
}

func HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK\n")
}
