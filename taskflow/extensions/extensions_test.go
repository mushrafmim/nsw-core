// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Lanka Software Foundation

package extensions

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/OpenNSW/core/taskflow/store"
)

type dummyExtension struct{}

func (d *dummyExtension) Execute(ctx context.Context, record *store.TaskRecord, payload map[string]any, properties json.RawMessage) error {
	return nil
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()

	// 1. Get unregistered
	_, exists := r.Get("non-existent")
	if exists {
		t.Fatal("expected unregistered extension not to exist")
	}

	// 2. Register new extension
	ext := &dummyExtension{}
	err := r.Register("my-validator", ext)
	if err != nil {
		t.Fatalf("expected registration to succeed, got: %v", err)
	}

	// 3. Get registered
	found, exists := r.Get("my-validator")
	if !exists {
		t.Fatal("expected registered extension to exist")
	}
	if found != ext {
		t.Error("returned extension does not match registered instance")
	}

	// 4. Duplicate registration error
	err = r.Register("my-validator", ext)
	if err == nil {
		t.Fatal("expected duplicate registration to fail, got nil")
	}
}
