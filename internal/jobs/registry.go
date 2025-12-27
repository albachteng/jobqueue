package jobs

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrHandlerNotFound = errors.New("no handler registered for job type")
	ErrHandlerExists   = errors.New("handler already registered for job type")
)

type Handler interface {
	Handle(ctx context.Context, envelope *Envelope) error
}

type HandlerFunc func(context.Context, *Envelope) error

func (f HandlerFunc) Handle(ctx context.Context, e *Envelope) error {
	return f(ctx, e)
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[JobType]Handler
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[JobType]Handler),
	}
}

func (r *Registry) Register(jobType JobType, handler Handler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("%w: %s", ErrHandlerExists, jobType)
	}

	r.handlers[jobType] = handler
	return nil
}

func (r *Registry) MustRegister(jobType JobType, handler Handler) {
	if err := r.Register(jobType, handler); err != nil {
		panic(err)
	}
}

func (r *Registry) Get(jobType JobType) (Handler, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	handler, exists := r.handlers[jobType]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotFound, jobType)
	}

	return handler, nil
}

func (r *Registry) Handle(ctx context.Context, envelope *Envelope) error {
	handler, err := r.Get(envelope.Type)
	if err != nil {
		return err
	}

	return handler.Handle(ctx, envelope)
}

func (r *Registry) RegisterFunc(jobType JobType, fn func(context.Context, *Envelope) error) error {
	return r.Register(jobType, HandlerFunc(fn))
}

func (r *Registry) Types() []JobType {
	r.mu.RLock()
	defer r.mu.RUnlock()

	types := make([]JobType, 0, len(r.handlers))
	for t := range r.handlers {
		types = append(types, t)
	}
	return types
}
