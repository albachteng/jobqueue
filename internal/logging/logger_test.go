package logging

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewDefault(t *testing.T) {
	logger := NewDefault()
	if logger == nil {
		t.Fatal("NewDefault returned nil logger")
	}

	// Should not panic
	logger.Info("test message")
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Level != slog.LevelInfo {
		t.Errorf("got level %v, want %v", cfg.Level, slog.LevelInfo)
	}

	if cfg.MaxSize != 100 {
		t.Errorf("got MaxSize %d, want 100", cfg.MaxSize)
	}

	if cfg.MaxBackups != 3 {
		t.Errorf("got MaxBackups %d, want 3", cfg.MaxBackups)
	}

	if cfg.MaxAge != 28 {
		t.Errorf("got MaxAge %d, want 28", cfg.MaxAge)
	}

	if !cfg.Compress {
		t.Error("expected Compress to be true")
	}

	if !cfg.JSON {
		t.Error("expected JSON to be true")
	}
}

func TestNewWithFileOutput(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	cfg := DefaultConfig()
	cfg.OutputFile = logFile
	logger := New(cfg)

	logger.Info("test message", "key", "value")

	if _, err := os.Stat(logFile); os.IsNotExist(err) {
		t.Fatalf("log file was not created: %s", logFile)
	}

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	logLine := string(content)
	if !strings.Contains(logLine, "test message") {
		t.Errorf("log line missing expected message: %s", logLine)
	}

	if !strings.Contains(logLine, `"key":"value"`) {
		t.Errorf("log line missing expected key-value pair: %s", logLine)
	}
}

func TestLogLevels(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	cfg := DefaultConfig()
	cfg.OutputFile = logFile
	cfg.Level = slog.LevelWarn
	logger := New(cfg)

	logger.Debug("debug message")
	logger.Info("info message")

	logger.Warn("warn message")
	logger.Error("error message")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	logContent := string(content)

	if strings.Contains(logContent, "debug message") {
		t.Error("debug message should not be logged at WARN level")
	}

	if strings.Contains(logContent, "info message") {
		t.Error("info message should not be logged at WARN level")
	}

	if !strings.Contains(logContent, "warn message") {
		t.Error("warn message should be logged")
	}

	if !strings.Contains(logContent, "error message") {
		t.Error("error message should be logged")
	}
}

func TestTextFormat(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	cfg := DefaultConfig()
	cfg.OutputFile = logFile
	cfg.JSON = false
	logger := New(cfg)

	logger.Info("test message", "key", "value")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	logLine := string(content)

	if strings.Contains(logLine, `{"`) {
		t.Errorf("expected text format, got JSON-like output: %s", logLine)
	}

	if !strings.Contains(logLine, "test message") {
		t.Errorf("log line missing message: %s", logLine)
	}

	if !strings.Contains(logLine, "key=value") {
		t.Errorf("log line missing key=value: %s", logLine)
	}
}

func TestJSONFormat(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	cfg := DefaultConfig()
	cfg.OutputFile = logFile
	cfg.JSON = true
	logger := New(cfg)

	logger.Info("test message", "key", "value")

	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("failed to read log file: %v", err)
	}

	logLine := string(content)

	if !strings.Contains(logLine, `"level":"INFO"`) {
		t.Errorf("missing JSON level field: %s", logLine)
	}

	if !strings.Contains(logLine, `"msg":"test message"`) {
		t.Errorf("missing JSON msg field: %s", logLine)
	}

	if !strings.Contains(logLine, `"key":"value"`) {
		t.Errorf("missing JSON key-value field: %s", logLine)
	}
}

func TestNilLogger(t *testing.T) {
	cfg := DefaultConfig()
	logger := New(cfg)

	if logger == nil {
		t.Fatal("New returned nil logger")
	}
}
