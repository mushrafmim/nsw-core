package common

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/OpenNSW/core/taskflow/extensions"
	"github.com/OpenNSW/core/taskflow/store"
	"github.com/xeipuuv/gojsonschema"
)

// JSONSchemaValidator is a built-in pre-resume extension that validates
// incoming CompleteTaskStep payloads against a configured JSON Schema.
type JSONSchemaValidator struct{}

// NewJSONSchemaValidator creates a new JSONSchemaValidator instance.
func NewJSONSchemaValidator() extensions.TaskExtension {
	return &JSONSchemaValidator{}
}

// Execute performs validation on the payload map.
// The properties argument must be a valid JSON Schema draft-4, draft-6, or draft-7 document.
func (v *JSONSchemaValidator) Execute(ctx context.Context, record *store.TaskRecord, payload map[string]any, properties json.RawMessage) error {
	if len(properties) == 0 || string(properties) == "null" || string(properties) == "{}" {
		// No schema configured; treat as no-op.
		return nil
	}

	schemaLoader := gojsonschema.NewBytesLoader(properties)
	documentLoader := gojsonschema.NewGoLoader(payload)

	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return fmt.Errorf("json schema validation engine error: %w", err)
	}

	if !result.Valid() {
		var errMsgs []string
		for _, desc := range result.Errors() {
			errMsgs = append(errMsgs, desc.String())
		}
		return fmt.Errorf("validation failed: %v", errMsgs)
	}

	return nil
}

var _ extensions.TaskExtension = (*JSONSchemaValidator)(nil)
