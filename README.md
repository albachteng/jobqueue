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

- `GET /` - Hello World
- `GET /health` - Health check
- `POST /jobs` - Enqueue a job (JSON payload with `type` and `payload`)
- `GET /jobs/{id}` - Get job status by ID
- `GET /jobs` - List all jobs (optional `?status=pending|processing|completed|failed`)

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

## Retry Logic

The job queue supports automatic retries with exponential backoff for failed jobs:

- **MaxRetries**: Maximum number of retries (default 0 = no retries)
- **Attempts**: Tracks how many times the job has been attempted
- **Backoff**: Exponential delay between retries (100ms base, 30s max)
  - Attempt 1: 100ms delay
  - Attempt 2: 200ms delay
  - Attempt 3: 400ms delay
  - Attempt 8+: 30s delay (capped)

Jobs are marked as `failed` only after exhausting all retries. The `Attempts` field in the job status shows how many times the job was attempted.

**Note**: Currently, retry configuration is set in code when registering job handlers. Future versions will support setting `MaxRetries` via the API.

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
