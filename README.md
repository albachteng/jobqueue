# Job Queue

A simple job queue server built with Go.

## Requirements

- Go 1.25 or higher

## Building

Build the server:

```bash
go build -o server ./cmd/server
```

## Running

Start the server (default port 8080):

```bash
go run ./cmd/server
```

Or set a custom port:

```bash
PORT=3000 go run ./cmd/server
```

## API Endpoints

### Core Endpoints
- `GET /` - Hello World
- `GET /health` - Health check

### Job Management
- `POST /jobs` - Enqueue a job (JSON payload with `type`, `payload`, and optional `priority`, `scheduled_at`, `max_retries`, `timeout`)
- `GET /jobs/{id}` - Get job status by ID
- `GET /jobs` - List all jobs (optional `?status=pending|processing|completed|failed|cancelled`)
- `DELETE /jobs/{id}` - Cancel a pending job

### Cron Jobs
- `POST /cron-jobs` - Create a recurring job schedule
- `GET /cron-jobs` - List all cron jobs
- `GET /cron-jobs/{id}` - Get a specific cron job
- `PUT /cron-jobs/{id}` - Update a cron job
- `DELETE /cron-jobs/{id}` - Delete a cron job

### End-to-End Example

```bash
# 1. Enqueue a job
RESPONSE=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": {"message": "hello world"}}')

echo "Enqueued: $RESPONSE"
# Output: {"job_id":"abc123...","status":"enqueued"}

# 2. Extract job ID
JOB_ID=$(echo $RESPONSE | jq -r '.job_id')

# 3. Check job status
curl -s http://localhost:8080/jobs/$JOB_ID | jq
# Output: {
#   "Envelope": {
#     "id": "abc123...",
#     "type": "echo",
#     "payload": {"message": "hello world"},
#     "attempts": 1,
#     "max_retries": 0,
#     ...
#   },
#   "Status": "completed",
#   "Error": "",
#   "StartedAt": "2025-12-27T...",
#   "FinishedAt": "2025-12-27T..."
# }

# 4. List all jobs
curl -s http://localhost:8080/jobs | jq

# 5. Filter by status
curl -s "http://localhost:8080/jobs?status=completed" | jq
```

Jobs are automatically processed by background workers. Use the status endpoints to track progress.

## Priority Queue

Jobs can be assigned priority values to control processing order. Higher priority jobs are processed first, with FIFO ordering within the same priority level.

- **Default priority**: 0
- **Higher numbers** = higher priority (e.g., priority 10 > priority 5 > priority 0)
- **Negative priorities** are supported (e.g., -5 for low-priority background tasks)

### Priority Examples

```bash
# High priority job (processed first)
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": "urgent task", "priority": 10}'

# Normal priority job
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": "normal task", "priority": 5}'

# Default priority (0) - omit priority field
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": "default priority task"}'

# Low priority background job
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": "background task", "priority": -5}'
```

When multiple jobs are queued, they will be processed in order:
1. Priority 10 jobs (FIFO within priority)
2. Priority 5 jobs (FIFO within priority)
3. Priority 0 jobs (FIFO within priority)
4. Priority -5 jobs (FIFO within priority)

## Scheduled Jobs

Jobs can be scheduled to run at a specific time in the future using the `scheduled_at` field. Jobs will not be processed until their scheduled time arrives.

- **Immediate jobs**: Omit `scheduled_at` or set to `null` (processed immediately)
- **Scheduled jobs**: Set `scheduled_at` to an RFC3339 timestamp (e.g., `2025-12-31T15:30:00Z`)
- **Processing order**: Immediate jobs are always processed before scheduled jobs
- Within scheduled jobs: ordered by scheduled time, then priority

### Scheduled Job Examples

```bash
# Schedule a job for 5 minutes from now
SCHEDULED_TIME=$(date -u -d '+5 minutes' +"%Y-%m-%dT%H:%M:%SZ")
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d "{
    \"type\": \"echo\",
    \"payload\": {\"message\": \"This runs in 5 minutes\"},
    \"scheduled_at\": \"$SCHEDULED_TIME\"
  }"

# Schedule a specific timestamp (New Year 2026)
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "echo",
    "payload": {"message": "Happy New Year 2026!"},
    "scheduled_at": "2026-01-01T00:00:00Z"
  }'

# Schedule with priority (high-priority job in 10 minutes)
SCHEDULED_TIME=$(date -u -d '+10 minutes' +"%Y-%m-%dT%H:%M:%SZ")
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d "{
    \"type\": \"echo\",
    \"payload\": {\"message\": \"Important scheduled task\"},
    \"scheduled_at\": \"$SCHEDULED_TIME\",
    \"priority\": 10
  }"

# Immediate job (will be processed before any scheduled jobs)
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "echo",
    "payload": {"message": "Process immediately"}
  }'
```

**Manual Testing Example:**

```bash
# Terminal 1: Start the server
go run ./cmd/server

# Terminal 2: Schedule a job for 30 seconds from now
SCHEDULED_TIME=$(date -u -d '+30 seconds' +"%Y-%m-%dT%H:%M:%SZ")
RESPONSE=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d "{
    \"type\": \"echo\",
    \"payload\": {\"message\": \"Delayed job test\"},
    \"scheduled_at\": \"$SCHEDULED_TIME\"
  }")

echo "Scheduled job: $RESPONSE"
JOB_ID=$(echo $RESPONSE | jq -r '.job_id')

# Check status immediately (should be pending)
curl -s http://localhost:8080/jobs/$JOB_ID | jq '.Status'
# Output: "pending"

# Wait 30+ seconds, then check again (should be completed)
sleep 35
curl -s http://localhost:8080/jobs/$JOB_ID | jq '.Status'
# Output: "completed"
```

Jobs scheduled in the past are processed immediately. Scheduled jobs persist across server restarts.

## Job Cancellation

Jobs can be cancelled while they are in pending status (before they start processing). Once a job begins processing, it cannot be cancelled and will run to completion.

### Cancellation Examples

```bash
# Enqueue a job
RESPONSE=$(curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "echo", "payload": {"message": "test"}}')

JOB_ID=$(echo $RESPONSE | jq -r '.job_id')

# Cancel the job (only works if job is still pending)
curl -X DELETE http://localhost:8080/jobs/$JOB_ID

# Response: {"job_id":"abc123...","status":"cancelled"}

# View cancelled jobs
curl -s "http://localhost:8080/jobs?status=cancelled" | jq
```

**Cancellation Rules:**
- ✅ **Pending jobs** - Can be cancelled
- ❌ **Processing jobs** - Cannot be cancelled (already executing)
- ❌ **Completed jobs** - Cannot be cancelled (already finished)
- ❌ **Failed/DLQ jobs** - Cannot be cancelled (already in terminal state)
- ❌ **Scheduled jobs** - Can be cancelled before their scheduled time (while still pending)

Cancelled jobs are excluded from processing and can be queried with `?status=cancelled`.

## Retry Logic

The job queue supports automatic retries with exponential backoff for failed jobs:

- **MaxRetries**: Maximum number of retries (default 0 = no retries)
- **Attempts**: Tracks how many times the job has been attempted
- **Backoff**: Exponential delay between retries (100ms base, 30s max)
  - Attempt 1: 100ms delay
  - Attempt 2: 200ms delay
  - Attempt 3: 400ms delay
  - Attempt 8+: 30s delay (capped)

Jobs are marked as `failed` and moved to the Dead Letter Queue (DLQ) only after exhausting all retries. The `Attempts` field in the job status shows how many times the job was attempted.

### Retry Examples

```bash
# Job with no retries (fails immediately on error)
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "echo",
    "payload": {"message": "fail fast"},
    "max_retries": 0
  }'

# Job with 3 retries (attempts up to 4 times total)
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "echo",
    "payload": {"message": "retry up to 3 times"},
    "max_retries": 3
  }'

# Critical job with many retries
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "echo",
    "payload": {"message": "important task"},
    "max_retries": 10,
    "priority": 10
  }'
```

**Note**: Different jobs can have different retry strategies. Combine with `priority` to ensure critical jobs are both retried more and processed first.

## Cron Jobs

Schedule recurring jobs with cron expressions. Cron jobs automatically create job instances at scheduled times.

### API Endpoints

- `POST /cron-jobs` - Create a new cron job
- `GET /cron-jobs` - List all cron jobs
- `GET /cron-jobs/{id}` - Get a specific cron job
- `PUT /cron-jobs/{id}` - Update a cron job
- `DELETE /cron-jobs/{id}` - Delete a cron job

### Creating Cron Jobs

```bash
# Run every 5 minutes
curl -X POST http://localhost:8080/cron-jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Periodic Task",
    "cron_expr": "*/5 * * * *",
    "job_type": "echo",
    "payload": {"message": "Running scheduled job"},
    "priority": 10,
    "max_retries": 3,
    "timeout": "30s",
    "enabled": true
  }'

# Daily at midnight
curl -X POST http://localhost:8080/cron-jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Daily Backup",
    "cron_expr": "0 0 * * *",
    "job_type": "backup",
    "payload": {"target": "/data"},
    "enabled": true
  }'

# Every Monday at 9am
curl -X POST http://localhost:8080/cron-jobs \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Weekly Report",
    "cron_expr": "0 9 * * 1",
    "job_type": "report",
    "payload": {"type": "weekly"},
    "enabled": true
  }'
```

### Cron Expression Format

Standard 5-field cron format (minute hour day month weekday):

```
* * * * *
│ │ │ │ │
│ │ │ │ └─── Weekday (0-6, 0=Sunday)
│ │ │ └───── Month (1-12)
│ │ └─────── Day of month (1-31)
│ └───────── Hour (0-23)
└─────────── Minute (0-59)
```

**Common expressions:**
- `*/5 * * * *` - Every 5 minutes
- `0 * * * *` - Every hour
- `0 0 * * *` - Daily at midnight
- `0 0 * * 0` - Weekly on Sunday
- `0 9 * * 1-5` - Weekdays at 9am
- `30 2 1 * *` - Monthly on 1st at 2:30am

### Managing Cron Jobs

```bash
# List all cron jobs
curl http://localhost:8080/cron-jobs | jq

# Get specific cron job
curl http://localhost:8080/cron-jobs/CRON_JOB_ID | jq

# Update cron job (disable)
curl -X PUT http://localhost:8080/cron-jobs/CRON_JOB_ID \
  -H "Content-Type: application/json" \
  -d '{
    "enabled": false
  }'

# Update cron schedule
curl -X PUT http://localhost:8080/cron-jobs/CRON_JOB_ID \
  -H "Content-Type: application/json" \
  -d '{
    "cron_expr": "*/10 * * * *",
    "enabled": true
  }'

# Delete cron job
curl -X DELETE http://localhost:8080/cron-jobs/CRON_JOB_ID
```

### Cron Job Behavior

- **Automatic scheduling**: Jobs are created automatically when cron expressions trigger
- **Disabled jobs**: Setting `enabled: false` prevents new job instances from being created
- **Next/Last run tracking**: System tracks when each cron job last ran and when it will run next
- **Job inheritance**: Created job instances inherit `priority`, `max_retries`, and `timeout` from the cron job
- **Persistence**: Cron jobs persist across server restarts
- **No overlap**: A single cron job definition can create multiple job instances

## Testing

Run all tests:

```bash
go test ./...
```

Run tests with verbose output:

```bash
go test -v ./...
```

Run tests with race detection:

```bash
go test -race ./...
```

## Logging

The server uses structured logging with slog and automatic log rotation via lumberjack.

### Configuration

Logs are written to both stdout and `logs/server.log` by default. The log file automatically rotates when file size exceeds 100MB.

- Keeps last 3 rotated files
- Rotated files are compressed
- Old logs deleted after 28 days

### Log Levels

Set via code (default is INFO):
```go
cfg := logging.DefaultConfig()
cfg.Level = slog.LevelDebug  // DEBUG, INFO, WARN, ERROR
```

### Output Format

Logs are in JSON format by default for easy parsing. Example:
```json
{"time":"2025-12-26T10:30:00Z","level":"INFO","msg":"server starting","address":":8080"}
{"time":"2025-12-26T10:30:05Z","level":"ERROR","msg":"job handler error","error":"timeout","job":{"id":"123"}}
```

## Development

### Linting

Run the linter:

```bash
golangci-lint run ./...
```

### CI/CD

GitHub Actions runs tests and linting on all pull requests.
