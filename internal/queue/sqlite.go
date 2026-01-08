package queue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	cronparser "github.com/robfig/cron/v3"

	"github.com/albachteng/jobqueue/internal/cron"
	"github.com/albachteng/jobqueue/internal/jobs"
)

// JobRecord represents a job with persistence metadata
type JobRecord struct {
	*jobs.Envelope
	Status      string
	LastError   string
	ProcessedAt *time.Time
}

// SQLiteQueue implements a persistent queue using SQLite
type SQLiteQueue struct {
	db *sql.DB
}

// NewSQLiteQueue creates a new SQLite-backed queue
func NewSQLiteQueue(dbPath string) (*SQLiteQueue, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	q := &SQLiteQueue{db: db}
	if err := q.initSchema(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to initialize schema: %w (close error: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	if err := q.recoverStuckJobs(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			return nil, fmt.Errorf("failed to recover stuck jobs: %w (close error: %v)", err, closeErr)
		}
		return nil, fmt.Errorf("failed to recover stuck jobs: %w", err)
	}

	return q, nil
}

// initSchema creates the jobs table if it doesn't exist and applies migrations
func (q *SQLiteQueue) initSchema() error {
	// Create base table structure (without indexes that depend on columns that might not exist)
	schema := `
	CREATE TABLE IF NOT EXISTS jobs (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		payload BLOB NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		priority INTEGER NOT NULL DEFAULT 0,
		attempts INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 3,
		last_error TEXT,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		processed_at TIMESTAMP,
		scheduled_at TIMESTAMP
	);
	`

	if _, err := q.db.Exec(schema); err != nil {
		return err
	}

	if err := q.applyMigrations(); err != nil {
		return err
	}

	indexSchema := `CREATE INDEX IF NOT EXISTS idx_status_priority_created ON jobs(status, priority DESC, created_at ASC);`
	if _, err := q.db.Exec(indexSchema); err != nil {
		return err
	}

	cronSchema := `
	CREATE TABLE IF NOT EXISTS cron_jobs (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		cron_expr TEXT NOT NULL,
		job_type TEXT NOT NULL,
		payload BLOB NOT NULL,
		priority INTEGER NOT NULL DEFAULT 0,
		max_retries INTEGER NOT NULL DEFAULT 0,
		timeout INTEGER NOT NULL DEFAULT 0,
		enabled BOOLEAN NOT NULL DEFAULT 1,
		next_run TIMESTAMP,
		last_run TIMESTAMP,
		created_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL
	);
	`
	if _, err := q.db.Exec(cronSchema); err != nil {
		return err
	}

	cronIndexSchema := `CREATE INDEX IF NOT EXISTS idx_cron_enabled_next_run ON cron_jobs(enabled, next_run);`
	if _, err := q.db.Exec(cronIndexSchema); err != nil {
		return err
	}

	return nil
}

// applyMigrations handles schema evolution for existing databases
func (q *SQLiteQueue) applyMigrations() error {
	var priorityExists bool
	err := q.db.QueryRow(`
		SELECT COUNT(*) > 0
		FROM pragma_table_info('jobs')
		WHERE name = 'priority'
	`).Scan(&priorityExists)

	if err != nil {
		return fmt.Errorf("failed to check for priority column: %w", err)
	}

	if !priorityExists {
		_, err := q.db.Exec(`ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0`)
		if err != nil {
			return fmt.Errorf("failed to add priority column: %w", err)
		}
	}

	var scheduledAtExists bool
	err = q.db.QueryRow(`
		SELECT COUNT(*) > 0
		FROM pragma_table_info('jobs')
		WHERE name = 'scheduled_at'
	`).Scan(&scheduledAtExists)

	if err != nil {
		return fmt.Errorf("failed to check for scheduled_at column: %w", err)
	}

	if !scheduledAtExists {
		_, err := q.db.Exec(`ALTER TABLE jobs ADD COLUMN scheduled_at TIMESTAMP`)
		if err != nil {
			return fmt.Errorf("failed to add scheduled_at column: %w", err)
		}
	}

	var timeoutExists bool
	err = q.db.QueryRow(`
		SELECT COUNT(*) > 0
		FROM pragma_table_info('jobs')
		WHERE name = 'timeout'
	`).Scan(&timeoutExists)

	if err != nil {
		return fmt.Errorf("failed to check for timeout column: %w", err)
	}

	if !timeoutExists {
		_, err := q.db.Exec(`ALTER TABLE jobs ADD COLUMN timeout INTEGER NOT NULL DEFAULT 0`)
		if err != nil {
			return fmt.Errorf("failed to add timeout column: %w", err)
		}
	}

	return nil
}

// recoverStuckJobs resets jobs that were processing when the server crashed
func (q *SQLiteQueue) recoverStuckJobs() error {
	query := `UPDATE jobs SET status = 'pending', updated_at = ? WHERE status = 'processing'`
	_, err := q.db.Exec(query, time.Now())
	return err
}

// Enqueue adds a job to the queue
func (q *SQLiteQueue) Enqueue(ctx context.Context, env *jobs.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	query := `
		INSERT INTO jobs (id, type, payload, status, priority, attempts, max_retries, created_at, updated_at, scheduled_at, timeout)
		VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := q.db.ExecContext(ctx, query,
		env.ID,
		env.Type,
		env.Payload,
		env.Priority,
		env.Attempts,
		env.MaxRetries,
		env.CreatedAt,
		time.Now(),
		env.ScheduledAt,
		int64(env.Timeout),
	)

	return err
}

// Dequeue retrieves and marks the next pending job as processing
func (q *SQLiteQueue) Dequeue(ctx context.Context) (*jobs.Envelope, error) {
	if err := ctx.Err(); err != nil {
		var zero *jobs.Envelope
		return zero, err
	}

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		var zero *jobs.Envelope
		return zero, err
	}
	defer func() {
		// Rollback is safe to call even if transaction was committed (returns sql.ErrTxDone)
		// In a defer, we cannot meaningfully handle or return errors
		_ = tx.Rollback() //nolint:errcheck
	}()

	// Find highest priority pending job that's ready to run
	// Immediate jobs (NULL scheduled_at) come first, then by scheduled time, then priority
	query := `
		SELECT id, type, payload, priority, attempts, max_retries, created_at, scheduled_at, timeout
		FROM jobs
		WHERE status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= ?)
		ORDER BY scheduled_at IS NULL DESC, scheduled_at ASC, priority DESC, created_at ASC
		LIMIT 1
	`

	var env jobs.Envelope
	var payload []byte
	var scheduledAt sql.NullTime
	var timeoutNs int64
	err = tx.QueryRowContext(ctx, query, time.Now()).Scan(
		&env.ID,
		&env.Type,
		&payload,
		&env.Priority,
		&env.Attempts,
		&env.MaxRetries,
		&env.CreatedAt,
		&scheduledAt,
		&timeoutNs,
	)

	if err == sql.ErrNoRows {
		return nil, ErrEmptyQueue
	}
	if err != nil {
		return nil, err
	}

	env.Payload = make([]byte, len(payload))
	copy(env.Payload, payload)

	if scheduledAt.Valid {
		env.ScheduledAt = &scheduledAt.Time
	}

	env.Timeout = time.Duration(timeoutNs)

	updateQuery := `UPDATE jobs SET status = 'processing', updated_at = ? WHERE id = ?`
	_, err = tx.ExecContext(ctx, updateQuery, time.Now(), env.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &env, nil
}

// CompleteJob marks a job as completed
func (q *SQLiteQueue) CompleteJob(ctx context.Context, jobID jobs.JobID) error {
	query := `
		UPDATE jobs
		SET status = 'completed', updated_at = ?, processed_at = ?
		WHERE id = ?
	`
	now := time.Now()
	_, err := q.db.ExecContext(ctx, query, now, now, jobID)
	return err
}

// FailJob marks a job as failed with an error message
func (q *SQLiteQueue) FailJob(ctx context.Context, jobID jobs.JobID, errorMsg string) error {
	query := `
		UPDATE jobs
		SET status = 'failed', last_error = ?, updated_at = ?, processed_at = ?
		WHERE id = ?
	`
	now := time.Now()
	_, err := q.db.ExecContext(ctx, query, errorMsg, now, now, jobID)
	return err
}

// RequeueJob updates a job and sets it back to pending for retry
func (q *SQLiteQueue) RequeueJob(ctx context.Context, env *jobs.Envelope) error {
	query := `
		UPDATE jobs
		SET status = 'pending', attempts = ?, priority = ?, timeout = ?, updated_at = ?
		WHERE id = ?
	`
	_, err := q.db.ExecContext(ctx, query, env.Attempts, env.Priority, int64(env.Timeout), time.Now(), env.ID)
	return err
}

// GetJob retrieves a job by ID
func (q *SQLiteQueue) GetJob(ctx context.Context, jobID jobs.JobID) (*JobRecord, bool) {
	query := `
		SELECT id, type, payload, status, priority, attempts, max_retries, last_error, created_at, processed_at, scheduled_at, timeout
		FROM jobs
		WHERE id = ?
	`

	var record JobRecord
	record.Envelope = &jobs.Envelope{}
	var payload []byte
	var processedAt sql.NullTime
	var lastError sql.NullString
	var scheduledAt sql.NullTime
	var timeoutNs int64

	err := q.db.QueryRowContext(ctx, query, jobID).Scan(
		&record.ID,
		&record.Type,
		&payload,
		&record.Status,
		&record.Priority,
		&record.Attempts,
		&record.MaxRetries,
		&lastError,
		&record.CreatedAt,
		&processedAt,
		&scheduledAt,
		&timeoutNs,
	)

	if err != nil {
		return nil, false
	}

	record.Payload = make([]byte, len(payload))
	copy(record.Payload, payload)

	if processedAt.Valid {
		record.ProcessedAt = &processedAt.Time
	}

	if lastError.Valid {
		record.LastError = lastError.String
	}

	if scheduledAt.Valid {
		record.ScheduledAt = &scheduledAt.Time
	}

	record.Timeout = time.Duration(timeoutNs)

	return &record, true
}

// ListJobsByStatus returns all jobs with a given status
func (q *SQLiteQueue) ListJobsByStatus(ctx context.Context, status string) []*JobRecord {
	query := `
		SELECT id, type, payload, status, priority, attempts, max_retries, last_error, created_at, processed_at, scheduled_at, timeout
		FROM jobs
		WHERE status = ?
		ORDER BY priority DESC, created_at DESC
	`

	rows, err := q.db.QueryContext(ctx, query, status)
	if err != nil {
		return []*JobRecord{}
	}
	defer func() {
		// Close is idempotent and safe to call multiple times
		// Iteration errors are available via rows.Err() after the loop
		_ = rows.Close() //nolint:errcheck
	}()

	var records []*JobRecord
	for rows.Next() {
		var record JobRecord
		record.Envelope = &jobs.Envelope{}
		var payload []byte
		var processedAt sql.NullTime
		var lastError sql.NullString
		var scheduledAt sql.NullTime
		var timeoutNs int64

		err := rows.Scan(
			&record.ID,
			&record.Type,
			&payload,
			&record.Status,
			&record.Priority,
			&record.Attempts,
			&record.MaxRetries,
			&lastError,
			&record.CreatedAt,
			&processedAt,
			&scheduledAt,
			&timeoutNs,
		)

		if err != nil {
			continue
		}

		record.Payload = make([]byte, len(payload))
		copy(record.Payload, payload)

		if processedAt.Valid {
			record.ProcessedAt = &processedAt.Time
		}

		if lastError.Valid {
			record.LastError = lastError.String
		}

		if scheduledAt.Valid {
			record.ScheduledAt = &scheduledAt.Time
		}

		record.Timeout = time.Duration(timeoutNs)

		records = append(records, &record)
	}

	return records
}

// MoveToDLQ moves a failed job to the dead letter queue
func (q *SQLiteQueue) MoveToDLQ(ctx context.Context, env *jobs.Envelope, errorMsg string) error {
	query := `
		UPDATE jobs
		SET status = 'dlq', last_error = ?, attempts = ?, updated_at = ?, processed_at = ?
		WHERE id = ?
	`
	now := time.Now()
	_, err := q.db.ExecContext(ctx, query, errorMsg, env.Attempts, now, now, env.ID)
	return err
}

// ListDLQJobs returns all jobs in the dead letter queue
func (q *SQLiteQueue) ListDLQJobs(ctx context.Context) []*JobRecord {
	return q.ListJobsByStatus(ctx, "dlq")
}

// RequeueDLQJob moves a job from DLQ back to pending status
func (q *SQLiteQueue) RequeueDLQJob(ctx context.Context, jobID jobs.JobID) error {
	record, exists := q.GetJob(ctx, jobID)
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if record.Status != "dlq" {
		return fmt.Errorf("job is not in DLQ")
	}

	query := `
		UPDATE jobs
		SET status = 'pending', attempts = 0, last_error = NULL, updated_at = ?
		WHERE id = ?
	`
	_, err := q.db.ExecContext(ctx, query, time.Now(), jobID)
	return err
}

// CancelJob marks a pending job as cancelled
func (q *SQLiteQueue) CancelJob(ctx context.Context, jobID jobs.JobID) error {
	record, exists := q.GetJob(ctx, jobID)
	if !exists {
		return ErrJobNotFound
	}

	if record.Status != "pending" {
		return ErrJobNotCancellable
	}

	query := `
		UPDATE jobs
		SET status = 'cancelled', updated_at = ?
		WHERE id = ?
	`
	_, err := q.db.ExecContext(ctx, query, time.Now(), jobID)
	return err
}

// CreateCronJob creates a new cron job
func (q *SQLiteQueue) CreateCronJob(ctx context.Context, cronJob *cron.CronJob) error {
	if cronJob.JobType == "" {
		return fmt.Errorf("job type cannot be empty")
	}

	parser := cronparser.NewParser(cronparser.Minute | cronparser.Hour | cronparser.Dom | cronparser.Month | cronparser.Dow)
	if _, err := parser.Parse(cronJob.CronExpr); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}

	query := `
		INSERT INTO cron_jobs (id, name, cron_expr, job_type, payload, priority, max_retries, timeout, enabled, next_run, last_run, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := q.db.ExecContext(ctx, query,
		cronJob.ID,
		cronJob.Name,
		cronJob.CronExpr,
		cronJob.JobType,
		cronJob.Payload,
		cronJob.Priority,
		cronJob.MaxRetries,
		int64(cronJob.Timeout),
		cronJob.Enabled,
		cronJob.NextRun,
		cronJob.LastRun,
		cronJob.CreatedAt,
		cronJob.UpdatedAt,
	)

	return err
}

// GetCronJob retrieves a cron job by ID
func (q *SQLiteQueue) GetCronJob(ctx context.Context, id cron.CronJobID) (*cron.CronJob, bool) {
	query := `
		SELECT id, name, cron_expr, job_type, payload, priority, max_retries, timeout, enabled, next_run, last_run, created_at, updated_at
		FROM cron_jobs
		WHERE id = ?
	`

	var cronJob cron.CronJob
	var payload []byte
	var nextRun sql.NullTime
	var lastRun sql.NullTime
	var timeoutNs int64

	err := q.db.QueryRowContext(ctx, query, id).Scan(
		&cronJob.ID,
		&cronJob.Name,
		&cronJob.CronExpr,
		&cronJob.JobType,
		&payload,
		&cronJob.Priority,
		&cronJob.MaxRetries,
		&timeoutNs,
		&cronJob.Enabled,
		&nextRun,
		&lastRun,
		&cronJob.CreatedAt,
		&cronJob.UpdatedAt,
	)

	if err != nil {
		return nil, false
	}

	cronJob.Payload = make([]byte, len(payload))
	copy(cronJob.Payload, payload)
	cronJob.Timeout = time.Duration(timeoutNs)

	if nextRun.Valid {
		cronJob.NextRun = &nextRun.Time
	}
	if lastRun.Valid {
		cronJob.LastRun = &lastRun.Time
	}

	return &cronJob, true
}

// ListCronJobs returns all cron jobs
func (q *SQLiteQueue) ListCronJobs(ctx context.Context) []*cron.CronJob {
	query := `
		SELECT id, name, cron_expr, job_type, payload, priority, max_retries, timeout, enabled, next_run, last_run, created_at, updated_at
		FROM cron_jobs
		ORDER BY name ASC
	`

	rows, err := q.db.QueryContext(ctx, query)
	if err != nil {
		return []*cron.CronJob{}
	}
	defer func() {
		_ = rows.Close() //nolint:errcheck
	}()

	var cronJobs []*cron.CronJob
	for rows.Next() {
		var cronJob cron.CronJob
		var payload []byte
		var nextRun sql.NullTime
		var lastRun sql.NullTime
		var timeoutNs int64

		err := rows.Scan(
			&cronJob.ID,
			&cronJob.Name,
			&cronJob.CronExpr,
			&cronJob.JobType,
			&payload,
			&cronJob.Priority,
			&cronJob.MaxRetries,
			&timeoutNs,
			&cronJob.Enabled,
			&nextRun,
			&lastRun,
			&cronJob.CreatedAt,
			&cronJob.UpdatedAt,
		)

		if err != nil {
			continue
		}

		cronJob.Payload = make([]byte, len(payload))
		copy(cronJob.Payload, payload)
		cronJob.Timeout = time.Duration(timeoutNs)

		if nextRun.Valid {
			cronJob.NextRun = &nextRun.Time
		}
		if lastRun.Valid {
			cronJob.LastRun = &lastRun.Time
		}

		cronJobs = append(cronJobs, &cronJob)
	}

	return cronJobs
}

// ListEnabledCronJobs returns all enabled cron jobs
func (q *SQLiteQueue) ListEnabledCronJobs(ctx context.Context) []*cron.CronJob {
	query := `
		SELECT id, name, cron_expr, job_type, payload, priority, max_retries, timeout, enabled, next_run, last_run, created_at, updated_at
		FROM cron_jobs
		WHERE enabled = 1
		ORDER BY next_run ASC
	`

	rows, err := q.db.QueryContext(ctx, query)
	if err != nil {
		return []*cron.CronJob{}
	}
	defer func() {
		_ = rows.Close() //nolint:errcheck
	}()

	var cronJobs []*cron.CronJob
	for rows.Next() {
		var cronJob cron.CronJob
		var payload []byte
		var nextRun sql.NullTime
		var lastRun sql.NullTime
		var timeoutNs int64

		err := rows.Scan(
			&cronJob.ID,
			&cronJob.Name,
			&cronJob.CronExpr,
			&cronJob.JobType,
			&payload,
			&cronJob.Priority,
			&cronJob.MaxRetries,
			&timeoutNs,
			&cronJob.Enabled,
			&nextRun,
			&lastRun,
			&cronJob.CreatedAt,
			&cronJob.UpdatedAt,
		)

		if err != nil {
			continue
		}

		cronJob.Payload = make([]byte, len(payload))
		copy(cronJob.Payload, payload)
		cronJob.Timeout = time.Duration(timeoutNs)

		if nextRun.Valid {
			cronJob.NextRun = &nextRun.Time
		}
		if lastRun.Valid {
			cronJob.LastRun = &lastRun.Time
		}

		cronJobs = append(cronJobs, &cronJob)
	}

	return cronJobs
}

// UpdateCronJob updates an existing cron job
func (q *SQLiteQueue) UpdateCronJob(ctx context.Context, cronJob *cron.CronJob) error {
	if cronJob.CronExpr != "" {
		parser := cronparser.NewParser(cronparser.Minute | cronparser.Hour | cronparser.Dom | cronparser.Month | cronparser.Dow)
		if _, err := parser.Parse(cronJob.CronExpr); err != nil {
			return fmt.Errorf("invalid cron expression: %w", err)
		}
	}

	_, exists := q.GetCronJob(ctx, cronJob.ID)
	if !exists {
		return fmt.Errorf("cron job not found")
	}

	query := `
		UPDATE cron_jobs
		SET name = ?, cron_expr = ?, job_type = ?, payload = ?, priority = ?, max_retries = ?, timeout = ?, enabled = ?, updated_at = ?
		WHERE id = ?
	`

	_, err := q.db.ExecContext(ctx, query,
		cronJob.Name,
		cronJob.CronExpr,
		cronJob.JobType,
		cronJob.Payload,
		cronJob.Priority,
		cronJob.MaxRetries,
		int64(cronJob.Timeout),
		cronJob.Enabled,
		time.Now(),
		cronJob.ID,
	)

	return err
}

// DeleteCronJob deletes a cron job by ID
func (q *SQLiteQueue) DeleteCronJob(ctx context.Context, id cron.CronJobID) error {
	_, exists := q.GetCronJob(ctx, id)
	if !exists {
		return fmt.Errorf("cron job not found")
	}

	query := `DELETE FROM cron_jobs WHERE id = ?`
	_, err := q.db.ExecContext(ctx, query, id)
	return err
}

// UpdateCronJobNextRun updates the next run time for a cron job
func (q *SQLiteQueue) UpdateCronJobNextRun(ctx context.Context, id cron.CronJobID, nextRun time.Time) error {
	query := `UPDATE cron_jobs SET next_run = ?, updated_at = ? WHERE id = ?`
	_, err := q.db.ExecContext(ctx, query, nextRun, time.Now(), id)
	return err
}

// UpdateCronJobLastRun updates the last run time for a cron job
func (q *SQLiteQueue) UpdateCronJobLastRun(ctx context.Context, id cron.CronJobID, lastRun time.Time) error {
	query := `UPDATE cron_jobs SET last_run = ?, updated_at = ? WHERE id = ?`
	_, err := q.db.ExecContext(ctx, query, lastRun, time.Now(), id)
	return err
}

// Close closes the database connection
func (q *SQLiteQueue) Close() error {
	return q.db.Close()
}
