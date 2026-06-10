package common

import (
	"context"
	"testing"

	"github.com/OpenNSW/core/taskflow/store"
)

func TestJSONSchemaValidator(t *testing.T) {
	validator := NewJSONSchemaValidator()

	schema := []byte(`{
		"type": "object",
		"properties": {
			"name": { "type": "string" },
			"age": { "type": "integer", "minimum": 18 }
		},
		"required": ["name", "age"]
	}`)

	// 1. Valid payload
	payload1 := map[string]any{
		"name": "Alice",
		"age":  30,
	}
	err := validator.Execute(context.Background(), &store.TaskRecord{}, payload1, schema)
	if err != nil {
		t.Fatalf("expected payload1 to be valid, got: %v", err)
	}

	// 2. Invalid payload (underage)
	payload2 := map[string]any{
		"name": "Bob",
		"age":  16,
	}
	err = validator.Execute(context.Background(), &store.TaskRecord{}, payload2, schema)
	if err == nil {
		t.Fatal("expected payload2 to fail validation (underage), got nil")
	}

	// 3. Invalid payload (missing required field 'name')
	payload3 := map[string]any{
		"age": 25,
	}
	err = validator.Execute(context.Background(), &store.TaskRecord{}, payload3, schema)
	if err == nil {
		t.Fatal("expected payload3 to fail validation (missing field 'name'), got nil")
	}

	// 4. Empty schema (no-op)
	err = validator.Execute(context.Background(), &store.TaskRecord{}, payload3, []byte(`{}`))
	if err != nil {
		t.Fatalf("expected empty schema to pass as no-op, got: %v", err)
	}

	// 5. Malformed JSON schema (validation engine error)
	err = validator.Execute(context.Background(), &store.TaskRecord{}, payload1, []byte(`{invalid-json`))
	if err == nil {
		t.Fatal("expected error due to malformed schema, got nil")
	}
}
