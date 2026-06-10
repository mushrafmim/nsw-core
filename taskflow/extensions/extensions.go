package extensions

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/OpenNSW/core/taskflow/store"
)

type ExecutionPhase string

const (
	PhasePreResume  ExecutionPhase = "PRE_RESUME"
	PhasePostResume ExecutionPhase = "POST_RESUME"
)

// TaskExtension defines the interface for task complete step interceptors.
type TaskExtension interface {
	// Execute runs the custom interceptor logic.
	// - phase: the current point in the CompleteTaskStep lifecycle.
	// - record: the database record representing the task execution context.
	// - payload: mutable map containing client submitted inputs (mutable only in PhasePreResume).
	// - properties: extension-specific parameters unmarshaled from template config.
	Execute(ctx context.Context, phase ExecutionPhase, record *store.TaskRecord, payload map[string]any, properties json.RawMessage) error
}

// Registry is a thread-safe registry of task extensions keyed by extension id.
type Registry struct {
	mu         sync.RWMutex
	extensions map[string]TaskExtension
}

// NewRegistry creates a new, empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		extensions: make(map[string]TaskExtension),
	}
}

// Register adds a new extension for a specific id. It returns an error if an extension with the same id already exists.
func (r *Registry) Register(id string, ext TaskExtension) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.extensions[id]; exists {
		return fmt.Errorf("extension with ID %q is already registered", id)
	}

	r.extensions[id] = ext
	return nil
}

// Get retrieves a registered extension by its id.
func (r *Registry) Get(id string) (TaskExtension, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ext, exists := r.extensions[id]
	return ext, exists
}
