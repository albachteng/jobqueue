package logging

import (
	"io"
	"log/slog"
	"os"

	"gopkg.in/natefinch/lumberjack.v2"
)

type Config struct {
	// Level is the minimum log level (DEBUG, INFO, WARN, ERROR)
	Level slog.Level

	// OutputFile is the path to the log file (optional - if empty, logs to stdout only)
	OutputFile string

	// MaxSize is the maximum size in megabytes before rotation (default 100MB)
	MaxSize int

	// MaxBackups is the maximum number of old log files to keep (default 3)
	MaxBackups int

	// MaxAge is the maximum days to keep old log files (default 28)
	MaxAge int

	// Compress determines if rotated logs should be compressed (default true)
	Compress bool

	// JSON determines if logs should be in JSON format (default true)
	JSON bool
}

func DefaultConfig() Config {
	return Config{
		Level:      slog.LevelInfo,
		MaxSize:    100,
		MaxBackups: 3,
		MaxAge:     28,
		Compress:   true,
		JSON:       true,
	}
}

func New(cfg Config) *slog.Logger {
	var writers []io.Writer
	writers = append(writers, os.Stdout)

	if cfg.OutputFile != "" {
		fileWriter := &lumberjack.Logger{
			Filename:   cfg.OutputFile,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
		writers = append(writers, fileWriter)
	}

	writer := io.MultiWriter(writers...)

	var handler slog.Handler
	if cfg.JSON {
		handler = slog.NewJSONHandler(writer, &slog.HandlerOptions{
			Level: cfg.Level,
		})
	} else {
		handler = slog.NewTextHandler(writer, &slog.HandlerOptions{
			Level: cfg.Level,
		})
	}

	return slog.New(handler)
}

func NewDefault() *slog.Logger {
	return New(DefaultConfig())
}
