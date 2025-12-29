package queue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"

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
		processed_at TIMESTAMP
	);
	`

	if _, err := q.db.Exec(schema); err != nil {
		return err
	}

	// Apply migrations for existing databases (this will add missing columns)
	if err := q.applyMigrations(); err != nil {
		return err
	}

	// Create indexes after migrations to ensure all columns exist
	indexSchema := `CREATE INDEX IF NOT EXISTS idx_status_priority_created ON jobs(status, priority DESC, created_at ASC);`
	if _, err := q.db.Exec(indexSchema); err != nil {
		return err
	}

	return nil
}

// applyMigrations handles schema evolution for existing databases
func (q *SQLiteQueue) applyMigrations() error {
	// Check if priority column exists (added in later version)
	var columnExists bool
	err := q.db.QueryRow(`
		SELECT COUNT(*) > 0
		FROM pragma_table_info('jobs')
		WHERE name = 'priority'
	`).Scan(&columnExists)

	if err != nil {
		return fmt.Errorf("failed to check for priority column: %w", err)
	}

	if !columnExists {
		// Add priority column with default value
		_, err := q.db.Exec(`ALTER TABLE jobs ADD COLUMN priority INTEGER NOT NULL DEFAULT 0`)
		if err != nil {
			return fmt.Errorf("failed to add priority column: %w", err)
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
		INSERT INTO jobs (id, type, payload, status, priority, attempts, max_retries, created_at, updated_at)
		VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?)
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

	// Find highest priority pending job (FIFO within same priority)
	query := `
		SELECT id, type, payload, priority, attempts, max_retries, created_at
		FROM jobs
		WHERE status = 'pending'
		ORDER BY priority DESC, created_at ASC
		LIMIT 1
	`

	var env jobs.Envelope
	var payload []byte
	err = tx.QueryRowContext(ctx, query).Scan(
		&env.ID,
		&env.Type,
		&payload,
		&env.Priority,
		&env.Attempts,
		&env.MaxRetries,
		&env.CreatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, ErrEmptyQueue
	}
	if err != nil {
		return nil, err
	}

	env.Payload = make([]byte, len(payload))
	copy(env.Payload, payload)

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
		SET status = 'pending', attempts = ?, priority = ?, updated_at = ?
		WHERE id = ?
	`
	_, err := q.db.ExecContext(ctx, query, env.Attempts, env.Priority, time.Now(), env.ID)
	return err
}

// GetJob retrieves a job by ID
func (q *SQLiteQueue) GetJob(ctx context.Context, jobID jobs.JobID) (*JobRecord, bool) {
	query := `
		SELECT id, type, payload, status, priority, attempts, max_retries, last_error, created_at, processed_at
		FROM jobs
		WHERE id = ?
	`

	var record JobRecord
	record.Envelope = &jobs.Envelope{}
	var payload []byte
	var processedAt sql.NullTime
	var lastError sql.NullString

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

	return &record, true
}

// ListJobsByStatus returns all jobs with a given status
func (q *SQLiteQueue) ListJobsByStatus(ctx context.Context, status string) []*JobRecord {
	query := `
		SELECT id, type, payload, status, priority, attempts, max_retries, last_error, created_at, processed_at
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
	// First verify the job is in DLQ
	record, exists := q.GetJob(ctx, jobID)
	if !exists {
		return fmt.Errorf("job not found: %s", jobID)
	}

	if record.Status != "dlq" {
		return fmt.Errorf("job is not in DLQ")
	}

	// Reset attempts and move back to pending
	query := `
		UPDATE jobs
		SET status = 'pending', attempts = 0, last_error = NULL, updated_at = ?
		WHERE id = ?
	`
	_, err := q.db.ExecContext(ctx, query, time.Now(), jobID)
	return err
}

// Close closes the database connection
func (q *SQLiteQueue) Close() error {
	return q.db.Close()
}
