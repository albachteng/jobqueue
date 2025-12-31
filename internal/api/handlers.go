package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/tracking"
)

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

func HandleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!\n")
}

func HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK\n")
}
