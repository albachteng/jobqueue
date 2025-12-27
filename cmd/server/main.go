package main

import (
	"context"
	"net/http"
	"os"

	"github.com/albachteng/jobqueue/internal/api"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/logging"
	"github.com/albachteng/jobqueue/internal/worker"
)

func main() {
	logCfg := logging.DefaultConfig()
	logCfg.OutputFile = "logs/server.log"
	logger := logging.New(logCfg)

	registry := jobs.NewRegistry()

	registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		logger.Info("echo handler", "payload", string(env.Payload))
		return nil
	}))

	srv := api.NewServer(registry, logger)

	dispatcher := worker.NewDispatcher(srv.Queue, 5, registry, srv.Tracker, logger)
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
	mux.HandleFunc("/", api.HandleRoot)
	mux.HandleFunc("/health", api.HandleHealth)
	mux.HandleFunc("POST /jobs", srv.HandleEnqueue)
	mux.HandleFunc("GET /jobs/{id}", srv.HandleGetJob)
	mux.HandleFunc("GET /jobs", srv.HandleListJobs)

	addr := ":" + port
	logger.Info("server starting", "address", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}
