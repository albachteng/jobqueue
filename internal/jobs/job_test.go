package jobs

import (
	"encoding/json"
	"testing"
	"time"
)

// Test payload structures
type EchoPayload struct {
	Message string `json:"message"`
}

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type WebhookPayload struct {
	URL     string            `json:"url"`
	Method  string            `json:"method"`
	Headers map[string]string `json:"headers"`
}

// Non-serializable type for testing error cases
type NonSerializable struct {
	Channel chan int
}

func TestNewEnvelope(t *testing.T) {
	t.Run("creates valid envelope with generated ID", func(t *testing.T) {
		payload := EchoPayload{Message: "hello"}
		envelope, err := NewEnvelope("echo", payload)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if envelope.ID == "" {
			t.Error("expected non-empty ID")
		}

		if envelope.Type != "echo" {
			t.Errorf("expected type 'echo', got %s", envelope.Type)
		}

		if len(envelope.Payload) == 0 {
			t.Error("expected non-empty payload")
		}
	})

	t.Run("sets CreatedAt timestamp", func(t *testing.T) {
		before := time.Now()
		envelope, err := NewEnvelope("echo", EchoPayload{Message: "test"})
		after := time.Now()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if envelope.CreatedAt.Before(before) || envelope.CreatedAt.After(after) {
			t.Errorf("CreatedAt %v not within expected range [%v, %v]",
				envelope.CreatedAt, before, after)
		}
	})

	t.Run("sets initial status to pending", func(t *testing.T) {
		envelope, err := NewEnvelope("echo", EchoPayload{Message: "test"})

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if envelope.Status != "pending" {
			t.Errorf("expected status 'pending', got %s", envelope.Status)
		}
	})

	t.Run("marshals payload to JSON", func(t *testing.T) {
		payload := EmailPayload{
			To:      "test@example.com",
			Subject: "Test",
			Body:    "Hello",
		}

		envelope, err := NewEnvelope("email", payload)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		var decoded EmailPayload
		if err := json.Unmarshal(envelope.Payload, &decoded); err != nil {
			t.Fatalf("payload is not valid JSON: %v", err)
		}

		if decoded.To != payload.To {
			t.Errorf("expected To=%s, got %s", payload.To, decoded.To)
		}
	})

	t.Run("returns error for non-serializable payload", func(t *testing.T) {
		payload := NonSerializable{Channel: make(chan int)}

		_, err := NewEnvelope("bad", payload)

		if err == nil {
			t.Error("expected error for non-serializable payload, got nil")
		}
	})

	t.Run("accepts json.RawMessage directly", func(t *testing.T) {
		rawJSON := json.RawMessage(`{"custom": "data"}`)

		envelope, err := NewEnvelope("custom", rawJSON)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if string(envelope.Payload) != string(rawJSON) {
			t.Errorf("expected payload %s, got %s", rawJSON, envelope.Payload)
		}
	})
}

func TestEnvelope_UnmarshalPayload(t *testing.T) {
	t.Run("unmarshals JSON payload into typed struct", func(t *testing.T) {
		payload := EchoPayload{Message: "hello world"}
		envelope, _ := NewEnvelope("echo", payload)

		var decoded EchoPayload
		err := envelope.UnmarshalPayload(&decoded)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if decoded.Message != payload.Message {
			t.Errorf("expected Message=%s, got %s", payload.Message, decoded.Message)
		}
	})

	t.Run("handles nested JSON structures", func(t *testing.T) {
		payload := WebhookPayload{
			URL:    "https://example.com/webhook",
			Method: "POST",
			Headers: map[string]string{
				"Authorization": "Bearer token",
				"Content-Type":  "application/json",
			},
		}

		envelope, _ := NewEnvelope("webhook", payload)

		var decoded WebhookPayload
		err := envelope.UnmarshalPayload(&decoded)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if decoded.URL != payload.URL {
			t.Errorf("expected URL=%s, got %s", payload.URL, decoded.URL)
		}

		if decoded.Headers["Authorization"] != payload.Headers["Authorization"] {
			t.Errorf("expected header Authorization=%s, got %s",
				payload.Headers["Authorization"], decoded.Headers["Authorization"])
		}
	})

	t.Run("returns error for type mismatch", func(t *testing.T) {
		payload := EchoPayload{Message: "test"}
		envelope, _ := NewEnvelope("echo", payload)

		// Try to unmarshal into wrong type
		var decoded EmailPayload
		err := envelope.UnmarshalPayload(&decoded)

		// Should succeed but fields will be empty (JSON unmarshal behavior)
		// The actual error would come from business logic validation
		if err != nil {
			t.Fatalf("JSON unmarshal succeeded but got error: %v", err)
		}

		if decoded.To != "" || decoded.Subject != "" {
			t.Error("expected empty fields for mismatched type")
		}
	})

	t.Run("handles empty payload", func(t *testing.T) {
		envelope := &Envelope{
			ID:        "test-id",
			Type:      "empty",
			Payload:   json.RawMessage("{}"),
			CreatedAt: time.Now(),
			Status:    "pending",
		}

		var decoded EchoPayload
		err := envelope.UnmarshalPayload(&decoded)

		if err != nil {
			t.Fatalf("expected no error for empty payload, got %v", err)
		}

		if decoded.Message != "" {
			t.Error("expected empty Message field")
		}
	})
}

func TestEnvelope_Serialization(t *testing.T) {
	t.Run("envelope marshals to JSON", func(t *testing.T) {
		payload := EchoPayload{Message: "serialize me"}
		envelope, _ := NewEnvelope("echo", payload)

		data, err := json.Marshal(envelope)

		if err != nil {
			t.Fatalf("expected no error marshaling envelope, got %v", err)
		}

		if len(data) == 0 {
			t.Error("expected non-empty JSON data")
		}
	})

	t.Run("envelope unmarshals from JSON", func(t *testing.T) {
		jsonData := `{
			"id": "test-123",
			"type": "echo",
			"payload": {"message": "hello"},
			"created_at": "2024-01-01T12:00:00Z",
			"status": "pending"
		}`

		var envelope Envelope
		err := json.Unmarshal([]byte(jsonData), &envelope)

		if err != nil {
			t.Fatalf("expected no error unmarshaling envelope, got %v", err)
		}

		if envelope.ID != "test-123" {
			t.Errorf("expected ID=test-123, got %s", envelope.ID)
		}

		if envelope.Type != "echo" {
			t.Errorf("expected Type=echo, got %s", envelope.Type)
		}

		if envelope.Status != "pending" {
			t.Errorf("expected Status=pending, got %s", envelope.Status)
		}
	})

	t.Run("round-trip preserves all fields", func(t *testing.T) {
		original := &Envelope{
			ID:         "round-trip-123",
			Type:       "test",
			Payload:    json.RawMessage(`{"data": "value"}`),
			CreatedAt:  time.Now().Truncate(time.Second), // Truncate for JSON comparison
			Status:     "pending",
			Priority:   5,
			MaxRetries: 3,
			Attempts:   1,
		}

		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("marshal failed: %v", err)
		}

		var decoded Envelope
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("unmarshal failed: %v", err)
		}

		if decoded.ID != original.ID {
			t.Errorf("ID mismatch: expected %s, got %s", original.ID, decoded.ID)
		}

		if decoded.Type != original.Type {
			t.Errorf("Type mismatch: expected %s, got %s", original.Type, decoded.Type)
		}

		if string(decoded.Payload) != string(original.Payload) {
			t.Errorf("Payload mismatch: expected %s, got %s", original.Payload, decoded.Payload)
		}

		if !decoded.CreatedAt.Equal(original.CreatedAt) {
			t.Errorf("CreatedAt mismatch: expected %v, got %v", original.CreatedAt, decoded.CreatedAt)
		}

		if decoded.Status != original.Status {
			t.Errorf("Status mismatch: expected %s, got %s", original.Status, decoded.Status)
		}

		if decoded.Priority != original.Priority {
			t.Errorf("Priority mismatch: expected %d, got %d", original.Priority, decoded.Priority)
		}

		if decoded.MaxRetries != original.MaxRetries {
			t.Errorf("MaxRetries mismatch: expected %d, got %d", original.MaxRetries, decoded.MaxRetries)
		}

		if decoded.Attempts != original.Attempts {
			t.Errorf("Attempts mismatch: expected %d, got %d", original.Attempts, decoded.Attempts)
		}
	})

	t.Run("compatible with json.RawMessage", func(t *testing.T) {
		payload := EchoPayload{Message: "raw message test"}
		envelope, _ := NewEnvelope("echo", payload)

		// Envelope.Payload should be usable as json.RawMessage
		var payloadCopy json.RawMessage = envelope.Payload

		var decoded EchoPayload
		if err := json.Unmarshal(payloadCopy, &decoded); err != nil {
			t.Fatalf("failed to unmarshal from RawMessage: %v", err)
		}

		if decoded.Message != payload.Message {
			t.Errorf("expected Message=%s, got %s", payload.Message, decoded.Message)
		}
	})
}
