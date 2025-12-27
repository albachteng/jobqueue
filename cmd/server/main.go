package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/logging"
	"github.com/albachteng/jobqueue/internal/queue"
)

type EnqueueRequest struct {
	Type    jobs.JobType    `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type EnqueueResponse struct {
	JobID  jobs.JobID `json:"job_id"`
	Status string     `json:"status"`
}

type server struct {
	queue    queue.Queue[*jobs.Envelope]
	registry *jobs.Registry
	tracker  *jobs.JobTracker
	logger   *slog.Logger
}

func newServer(registry *jobs.Registry, logger *slog.Logger) *server {
	return &server{
		queue:    queue.NewInMemoryQueue[*jobs.Envelope](),
		registry: registry,
		tracker:  jobs.NewJobTracker(),
		logger:   logger,
	}
}

func (s *server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if _, err := s.registry.Get(req.Type); err != nil {
		http.Error(w, fmt.Sprintf("unknown job type: %s", req.Type), http.StatusBadRequest)
		return
	}

	envelope, err := jobs.NewEnvelope(req.Type, req.Payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.tracker.Register(envelope)

	if err := s.queue.Enqueue(r.Context(), envelope); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.logger.Info("job enqueued",
		"job_id", envelope.ID,
		"job_type", envelope.Type)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EnqueueResponse{
		JobID:  envelope.ID,
		Status: "enqueued",
	})
}

func (s *server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	jobID := jobs.JobID(r.PathValue("id"))

	info, exists := s.tracker.Get(jobID)
	if !exists {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(info)
}

func (s *server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	statusFilter := r.URL.Query().Get("status")

	var jobList []*jobs.JobInfo
	if statusFilter != "" {
		jobList = s.tracker.ListByStatus(jobs.JobStatus(statusFilter))
	} else {
		jobList = s.tracker.List()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jobList)
}

func main() {
	logCfg := logging.DefaultConfig()
	logCfg.OutputFile = "logs/server.log"
	logger := logging.New(logCfg)

	registry := jobs.NewRegistry()

	registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		logger.Info("echo handler", "payload", string(env.Payload))
		return nil
	}))

	srv := newServer(registry, logger)

	dispatcher := queue.NewDispatcher(srv.queue, 5, registry, srv.tracker, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := dispatcher.Start(ctx); err != nil {
		logger.Error("failed to start dispatcher", "error", err)
		os.Exit(1)
	}
	defer dispatcher.Stop()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRoot)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("POST /jobs", srv.handleEnqueue)
	mux.HandleFunc("GET /jobs/{id}", srv.handleGetJob)
	mux.HandleFunc("GET /jobs", srv.handleListJobs)

	addr := ":" + port
	logger.Info("server starting", "address", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello, World!\n")
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK\n")
}
