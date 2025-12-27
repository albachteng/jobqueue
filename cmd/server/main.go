package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
}

func newServer(registry *jobs.Registry) *server {
	return &server{
		queue:    queue.NewInMemoryQueue[*jobs.Envelope](),
		registry: registry,
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

	if err := s.queue.Enqueue(r.Context(), envelope); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EnqueueResponse{
		JobID:  envelope.ID,
		Status: "enqueued",
	})
}

func (s *server) handleDequeue(w http.ResponseWriter, r *http.Request) {
	envelope, err := s.queue.Dequeue(r.Context())
	if err != nil {
		if errors.Is(err, queue.ErrEmptyQueue) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(envelope)
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

	srv := newServer(registry)

	dispatcher := queue.NewDispatcher(srv.queue, 5, registry, logger)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dispatcher.Start(ctx)
	defer dispatcher.Stop()

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", handleRoot)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("POST /jobs", srv.handleEnqueue)
	mux.HandleFunc("GET /jobs", srv.handleDequeue)

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
