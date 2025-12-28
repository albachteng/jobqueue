package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/albachteng/jobqueue/internal/api"
	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/logging"
	"github.com/albachteng/jobqueue/internal/shutdown"
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

	httpServer := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	shutdownManager := shutdown.NewManagerWithTimeout(context.Background(), 30*time.Second, logger)

	shutdownManager.RegisterTask("http-server", func(ctx context.Context) error {
		logger.Info("shutting down HTTP server")
		return httpServer.Shutdown(ctx)
	})

	shutdownManager.RegisterTask("dispatcher", func(ctx context.Context) error {
		logger.Info("shutting down dispatcher")
		cancel() // Cancel dispatcher context
		dispatcher.Stop()
		return nil
	})

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		logger.Info("server starting", "address", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-sigChan
	logger.Info("shutdown signal received")

	shutdownManager.Shutdown()
	shutdownManager.Wait()

	if errors := shutdownManager.Errors(); len(errors) > 0 {
		logger.Error("shutdown completed with errors", "error_count", len(errors))
		for _, err := range errors {
			logger.Error("shutdown error", "error", err)
		}
		os.Exit(1)
	}

	logger.Info("shutdown complete")
}
