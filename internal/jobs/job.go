package jobs

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

type JobID string

type JobType string

type Job[T any] struct {
	ID        JobID
	Payload   T
	CreatedAt time.Time
}

type Envelope struct {
	ID          JobID           `json:"id"`
	Type        JobType         `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	CreatedAt   time.Time       `json:"created_at"`
	Status      string          `json:"status,omitempty"`
	Priority    int             `json:"priority,omitempty"`
	MaxRetries  int             `json:"max_retries,omitempty"`
	Attempts    int             `json:"attempts,omitempty"`
	ScheduledAt *time.Time      `json:"scheduled_at,omitempty"`
}

func NewEnvelope(jobType JobType, payload any) (*Envelope, error) {
	if payload == nil {
		return nil, fmt.Errorf("payload cannot be nil")
	}

	var payloadBytes json.RawMessage
	var err error

	switch p := payload.(type) {
	case json.RawMessage:
		payloadBytes = p
	default:
		payloadBytes, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
	}

	return &Envelope{
		ID:        JobID(generateID()),
		Type:      jobType,
		Payload:   payloadBytes,
		CreatedAt: time.Now(),
		Status:    "pending",
	}, nil
}

func (e *Envelope) UnmarshalPayload(v any) error {
	return json.Unmarshal(e.Payload, v)
}

func generateID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
