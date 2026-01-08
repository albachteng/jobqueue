package api

import (
	"context"
	"log/slog"
	"os"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
)

func NewTestServer(q queue.PersistentQueue) *Server {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	registry := jobs.NewRegistry()
	registry.MustRegister("echo", jobs.HandlerFunc(func(ctx context.Context, env *jobs.Envelope) error {
		return nil
	}))

	return &Server{
		Queue:    q,
		Registry: registry,
		Logger:   logger,
	}
}
