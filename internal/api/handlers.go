package api

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/albachteng/jobqueue/internal/jobs"
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

	s.Tracker.Register(envelope)

	if err := s.Queue.Enqueue(r.Context(), envelope); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.Logger.Info("job enqueued",
		"job_id", envelope.ID,
		"job_type", envelope.Type)

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

func HandleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!\n")
}

func HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK\n")
}
