package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestRegistry_Register(t *testing.T) {
	t.Run("registers handler for job type", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		err := registry.Register("test", handler)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		retrieved, err := registry.Get("test")
		if err != nil {
			t.Fatalf("expected to retrieve handler, got error: %v", err)
		}

		if retrieved == nil {
			t.Error("expected non-nil handler")
		}
	})

	t.Run("returns error when registering duplicate type", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		registry.Register("duplicate", handler)

		err := registry.Register("duplicate", handler)

		if err == nil {
			t.Error("expected error for duplicate registration, got nil")
		}

		if !errors.Is(err, ErrHandlerExists) {
			t.Errorf("expected ErrHandlerExists, got %v", err)
		}
	})

	t.Run("thread-safe concurrent registration", func(t *testing.T) {
		registry := NewRegistry()

		var wg sync.WaitGroup
		numGoroutines := 10

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				jobType := JobType(string(rune('a' + id)))
				handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
					return nil
				})

				err := registry.Register(jobType, handler)
				if err != nil {
					t.Errorf("concurrent registration failed for %s: %v", jobType, err)
				}
			}(i)
		}

		wg.Wait()

		types := registry.Types()
		if len(types) != numGoroutines {
			t.Errorf("expected %d types registered, got %d", numGoroutines, len(types))
		}
	})
}

func TestRegistry_Get(t *testing.T) {
	t.Run("retrieves registered handler by type", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		registry.Register("echo", handler)

		retrieved, err := registry.Get("echo")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if retrieved == nil {
			t.Error("expected non-nil handler")
		}
	})

	t.Run("returns ErrHandlerNotFound for unknown type", func(t *testing.T) {
		registry := NewRegistry()

		_, err := registry.Get("nonexistent")

		if err == nil {
			t.Error("expected error for unknown type, got nil")
		}

		if !errors.Is(err, ErrHandlerNotFound) {
			t.Errorf("expected ErrHandlerNotFound, got %v", err)
		}
	})

	t.Run("thread-safe concurrent reads", func(t *testing.T) {
		registry := NewRegistry()

		// Register a handler
		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})
		registry.Register("concurrent", handler)

		var wg sync.WaitGroup
		numGoroutines := 50

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				_, err := registry.Get("concurrent")
				if err != nil {
					t.Errorf("concurrent Get failed: %v", err)
				}
			}()
		}

		wg.Wait()
	})
}

func TestRegistry_Handle(t *testing.T) {
	t.Run("routes envelope to correct handler", func(t *testing.T) {
		registry := NewRegistry()

		var handledType JobType
		var handledEnvelope *Envelope

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			handledType = env.Type
			handledEnvelope = env
			return nil
		})

		registry.Register("test", handler)

		envelope := &Envelope{
			ID:     "test-123",
			Type:   "test",
			Status: "pending",
		}

		err := registry.Handle(context.Background(), envelope)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if handledType != "test" {
			t.Errorf("expected type 'test', got %s", handledType)
		}

		if handledEnvelope.ID != envelope.ID {
			t.Error("handler did not receive correct envelope")
		}
	})

	t.Run("handler receives full envelope", func(t *testing.T) {
		registry := NewRegistry()

		var receivedEnvelope *Envelope

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			receivedEnvelope = env
			return nil
		})

		registry.Register("full", handler)

		envelope := &Envelope{
			ID:         "full-123",
			Type:       "full",
			Payload:    []byte(`{"data": "value"}`),
			Status:     "pending",
			Priority:   5,
			MaxRetries: 3,
		}

		registry.Handle(context.Background(), envelope)

		if receivedEnvelope.ID != envelope.ID {
			t.Error("handler did not receive envelope ID")
		}

		if receivedEnvelope.Priority != envelope.Priority {
			t.Error("handler did not receive envelope Priority")
		}

		if string(receivedEnvelope.Payload) != string(envelope.Payload) {
			t.Error("handler did not receive envelope Payload")
		}
	})

	t.Run("returns handler errors unchanged", func(t *testing.T) {
		registry := NewRegistry()

		expectedErr := errors.New("handler error")
		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return expectedErr
		})

		registry.Register("error", handler)

		envelope := &Envelope{
			ID:     "error-123",
			Type:   "error",
			Status: "pending",
		}

		err := registry.Handle(context.Background(), envelope)

		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("returns ErrHandlerNotFound for unknown type", func(t *testing.T) {
		registry := NewRegistry()

		envelope := &Envelope{
			ID:     "unknown-123",
			Type:   "unknown",
			Status: "pending",
		}

		err := registry.Handle(context.Background(), envelope)

		if err == nil {
			t.Error("expected error for unknown type, got nil")
		}

		if !errors.Is(err, ErrHandlerNotFound) {
			t.Errorf("expected ErrHandlerNotFound, got %v", err)
		}
	})
}

func TestRegistry_RegisterFunc(t *testing.T) {
	t.Run("registers function as handler", func(t *testing.T) {
		registry := NewRegistry()

		var called bool
		fn := func(ctx context.Context, env *Envelope) error {
			called = true
			return nil
		}

		err := registry.RegisterFunc("func", fn)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		envelope := &Envelope{
			ID:     "func-123",
			Type:   "func",
			Status: "pending",
		}

		registry.Handle(context.Background(), envelope)

		if !called {
			t.Error("expected function to be called")
		}
	})

	t.Run("function receives envelope", func(t *testing.T) {
		registry := NewRegistry()

		var receivedID JobID
		fn := func(ctx context.Context, env *Envelope) error {
			receivedID = env.ID
			return nil
		}

		registry.RegisterFunc("recv", fn)

		envelope := &Envelope{
			ID:     "recv-123",
			Type:   "recv",
			Status: "pending",
		}

		registry.Handle(context.Background(), envelope)

		if receivedID != envelope.ID {
			t.Errorf("expected ID %s, got %s", envelope.ID, receivedID)
		}
	})

	t.Run("function errors propagate correctly", func(t *testing.T) {
		registry := NewRegistry()

		expectedErr := errors.New("function error")
		fn := func(ctx context.Context, env *Envelope) error {
			return expectedErr
		}

		registry.RegisterFunc("err", fn)

		envelope := &Envelope{
			ID:     "err-123",
			Type:   "err",
			Status: "pending",
		}

		err := registry.Handle(context.Background(), envelope)

		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})
}

func TestRegistry_Types(t *testing.T) {
	t.Run("returns all registered job types", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		registry.Register("type1", handler)
		registry.Register("type2", handler)
		registry.Register("type3", handler)

		types := registry.Types()

		if len(types) != 3 {
			t.Fatalf("expected 3 types, got %d", len(types))
		}

		typeMap := make(map[JobType]bool)
		for _, typ := range types {
			typeMap[typ] = true
		}

		if !typeMap["type1"] || !typeMap["type2"] || !typeMap["type3"] {
			t.Error("not all registered types were returned")
		}
	})

	t.Run("empty list for new registry", func(t *testing.T) {
		registry := NewRegistry()

		types := registry.Types()

		if len(types) != 0 {
			t.Errorf("expected empty types list, got %d types", len(types))
		}
	})

	t.Run("thread-safe concurrent access", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		for i := 0; i < 5; i++ {
			jobType := JobType(string(rune('a' + i)))
			registry.Register(jobType, handler)
		}

		var wg sync.WaitGroup
		numGoroutines := 20

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				types := registry.Types()
				if len(types) != 5 {
					t.Errorf("expected 5 types, got %d", len(types))
				}
			}()
		}

		wg.Wait()
	})
}

func TestRegistry_MustRegister(t *testing.T) {
	t.Run("registers handler successfully", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		registry.MustRegister("must", handler)

		_, err := registry.Get("must")
		if err != nil {
			t.Errorf("expected handler to be registered, got error: %v", err)
		}
	})

	t.Run("panics on duplicate registration", func(t *testing.T) {
		registry := NewRegistry()

		handler := HandlerFunc(func(ctx context.Context, env *Envelope) error {
			return nil
		})

		registry.MustRegister("panic", handler)

		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for duplicate registration")
			}
		}()

		registry.MustRegister("panic", handler)
	})
}
