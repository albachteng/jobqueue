package queue

import (
	"context"
	"log/slog"
	"sync"
)

type testLogRecord struct {
	level   slog.Level
	message string
	attrs   map[string]any
}

type testLogger struct {
	mu      sync.Mutex
	records []testLogRecord
}

func newTestLogger() *testLogger {
	return &testLogger{
		records: make([]testLogRecord, 0),
	}
}

func (l *testLogger) handler() slog.Handler {
	return &testLogHandler{logger: l}
}

func (l *testLogger) getRecords() []testLogRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	records := make([]testLogRecord, len(l.records))
	copy(records, l.records)
	return records
}

func (l *testLogger) hasMessage(msg string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.records {
		if r.message == msg {
			return true
		}
	}
	return false
}

func (l *testLogger) hasAttr(key string, value any) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.records {
		if v, ok := r.attrs[key]; ok && v == value {
			return true
		}
	}
	return false
}

func (l *testLogger) hasError() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range l.records {
		if r.level == slog.LevelError {
			return true
		}
	}
	return false
}

func (l *testLogger) errorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	count := 0
	for _, r := range l.records {
		if r.level == slog.LevelError {
			count++
		}
	}
	return count
}

func (l *testLogger) clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = make([]testLogRecord, 0)
}

type testLogHandler struct {
	logger *testLogger
	attrs  []slog.Attr
	groups []string
}

func (h *testLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *testLogHandler) Handle(ctx context.Context, record slog.Record) error {
	h.logger.mu.Lock()
	defer h.logger.mu.Unlock()

	attrs := make(map[string]any)
	for _, attr := range h.attrs {
		attrs[attr.Key] = attr.Value.Any()
	}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})

	h.logger.records = append(h.logger.records, testLogRecord{
		level:   record.Level,
		message: record.Message,
		attrs:   attrs,
	})
	return nil
}

func (h *testLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &testLogHandler{
		logger: h.logger,
		attrs:  newAttrs,
		groups: h.groups,
	}
}

func (h *testLogHandler) WithGroup(name string) slog.Handler {
	newGroups := make([]string, len(h.groups)+1)
	copy(newGroups, h.groups)
	newGroups[len(h.groups)] = name
	return &testLogHandler{
		logger: h.logger,
		attrs:  h.attrs,
		groups: newGroups,
	}
}
