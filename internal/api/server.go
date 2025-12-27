package api

import (
	"log/slog"

	"github.com/albachteng/jobqueue/internal/jobs"
	"github.com/albachteng/jobqueue/internal/queue"
	"github.com/albachteng/jobqueue/internal/tracking"
)

type Server struct {
	Queue    queue.Queue[*jobs.Envelope]
	Registry *jobs.Registry
	Tracker  *tracking.JobTracker
	Logger   *slog.Logger
}

func NewServer(registry *jobs.Registry, logger *slog.Logger) *Server {
	return &Server{
		Queue:    queue.NewInMemoryQueue[*jobs.Envelope](),
		Registry: registry,
		Tracker:  tracking.NewJobTracker(),
		Logger:   logger,
	}
}
