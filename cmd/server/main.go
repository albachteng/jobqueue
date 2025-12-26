package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/albachteng/jobqueue/internal/logging"
	"github.com/albachteng/jobqueue/internal/queue"
)

type server struct {
	queue queue.Queue[map[string]string]
}

func newServer() *server {
	return &server{
		queue: queue.NewInMemoryQueue[map[string]string](),
	}
}

func (s *server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var payload map[string]string
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	if err := s.queue.Enqueue(ctx, payload); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "enqueued"})
}

func (s *server) handleDequeue(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	job, err := s.queue.Dequeue(ctx)
	if err != nil {
		if errors.Is(err, queue.ErrEmptyQueue) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(job)
}

func main() {
	logCfg := logging.DefaultConfig()
	logCfg.OutputFile = "logs/server.log"
	logger := logging.New(logCfg)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := newServer()

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
