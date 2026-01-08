package cron

import (
	"context"
	"log/slog"
	"time"

	cronparser "github.com/robfig/cron/v3"

	"github.com/albachteng/jobqueue/internal/jobs"
)

type CronQueue interface {
	Enqueue(ctx context.Context, env *jobs.Envelope) error
	ListEnabledCronJobs(ctx context.Context) []*CronJob
	UpdateCronJobNextRun(ctx context.Context, id CronJobID, nextRun time.Time) error
	UpdateCronJobLastRun(ctx context.Context, id CronJobID, lastRun time.Time) error
}

type Scheduler struct {
	queue  CronQueue
	logger *slog.Logger
	parser cronparser.Parser
}

func NewScheduler(queue CronQueue, logger *slog.Logger) *Scheduler {
	if logger == nil {
		logger = slog.Default()
	}

	return &Scheduler{
		queue:  queue,
		logger: logger,
		parser: cronparser.NewParser(cronparser.Minute | cronparser.Hour | cronparser.Dom | cronparser.Month | cronparser.Dow),
	}
}

func (s *Scheduler) CalculateNextRun(cronExpr string, from time.Time) (time.Time, error) {
	schedule, err := s.parser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, err
	}

	return schedule.Next(from), nil
}

func (s *Scheduler) ProcessDueJobs(ctx context.Context) error {
	cronJobs := s.queue.ListEnabledCronJobs(ctx)
	now := time.Now()

	for _, cronJob := range cronJobs {
		if cronJob.NextRun == nil {
			nextRun, err := s.CalculateNextRun(cronJob.CronExpr, now)
			if err != nil {
				s.logger.Error("failed to calculate next run for cron job",
					"cron_job_id", cronJob.ID,
					"error", err)
				continue
			}

			if err := s.queue.UpdateCronJobNextRun(ctx, cronJob.ID, nextRun); err != nil {
				s.logger.Error("failed to update next run time",
					"cron_job_id", cronJob.ID,
					"error", err)
			}
			continue
		}

		if cronJob.NextRun.After(now) {
			continue
		}

		env, err := jobs.NewEnvelope(cronJob.JobType, cronJob.Payload)
		if err != nil {
			s.logger.Error("failed to create envelope from cron job",
				"cron_job_id", cronJob.ID,
				"error", err)
			continue
		}

		env.Priority = cronJob.Priority
		env.MaxRetries = cronJob.MaxRetries
		env.Timeout = cronJob.Timeout

		if err := s.queue.Enqueue(ctx, env); err != nil {
			s.logger.Error("failed to enqueue job from cron",
				"cron_job_id", cronJob.ID,
				"job_id", env.ID,
				"error", err)
			continue
		}

		s.logger.Info("enqueued job from cron schedule",
			"cron_job_id", cronJob.ID,
			"cron_name", cronJob.Name,
			"job_id", env.ID,
			"job_type", env.Type)

		nextRun, err := s.CalculateNextRun(cronJob.CronExpr, now)
		if err != nil {
			s.logger.Error("failed to calculate next run",
				"cron_job_id", cronJob.ID,
				"error", err)
			continue
		}

		if err := s.queue.UpdateCronJobNextRun(ctx, cronJob.ID, nextRun); err != nil {
			s.logger.Error("failed to update next run time",
				"cron_job_id", cronJob.ID,
				"error", err)
		}

		if err := s.queue.UpdateCronJobLastRun(ctx, cronJob.ID, now); err != nil {
			s.logger.Error("failed to update last run time",
				"cron_job_id", cronJob.ID,
				"error", err)
		}
	}

	return nil
}
