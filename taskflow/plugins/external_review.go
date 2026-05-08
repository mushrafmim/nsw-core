package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/OpenNSW/nsw-task-flow/store"
)

// HTTPDispatcher defines the function signature for executing external system integrations.
type HTTPDispatcher func(ctx context.Context, url string, taskID string, payload map[string]any) error

// DefaultHTTPDispatcher sends the payload as-is with no envelope.
// Callers that need a specific request shape should provide a custom dispatcher.
func DefaultHTTPDispatcher(ctx context.Context, url string, taskID string, payload map[string]any) error {
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal dispatch payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("failed to create http request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Task-ID", taskID) // carry task ID as a header, not in the body

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http dispatch failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("external system returned %d: %s", resp.StatusCode, body)
	}

	return nil
}

// ExternalReviewPlugin manages asynchronous delegation of task steps to third-party government agencies.
type ExternalReviewPlugin struct {
	dispatcher HTTPDispatcher
}

// NewExternalReviewPlugin returns a plugin with a custom or default HTTP dispatcher.
func NewExternalReviewPlugin(dispatcher HTTPDispatcher) *ExternalReviewPlugin {
	if dispatcher == nil {
		dispatcher = DefaultHTTPDispatcher
	}
	return &ExternalReviewPlugin{
		dispatcher: dispatcher,
	}
}

func (p *ExternalReviewPlugin) Name() string {
	return "generic_external_review"
}

// ExternalReviewConfig holds properties decoded from the TaskTemplate's JSON configuration.
type ExternalReviewConfig struct {
	ExternalURL         string `json:"external_url"`
	ReviewerJsonFormsID string `json:"reviewer_jsonforms_id,omitempty"`
}

func (p *ExternalReviewPlugin) Execute(ctx PluginContext, configRaw json.RawMessage) error {
	var cfg ExternalReviewConfig
	if err := json.Unmarshal(configRaw, &cfg); err != nil {
		return fmt.Errorf("failed to parse external review plugin config: %w", err)
	}

	if cfg.ExternalURL == "" {
		return fmt.Errorf("missing 'external_url' in external review plugin config")
	}

	if cfg.ReviewerJsonFormsID != "" {
		ctx.Record.ReviewerFormID = cfg.ReviewerJsonFormsID
	}

	ctx.Record.Status = "QUEUED_EXTERNALLY"
	log.Printf("[Plugin: generic_external_review] Dispatching task %s to external URL: %s", ctx.Record.TaskID, cfg.ExternalURL)

	err := p.dispatcher(ctx.Context, cfg.ExternalURL, ctx.Record.TaskID, ctx.Record.Data)
	if err != nil {
		return fmt.Errorf("external dispatch failed: %w", err)
	}

	log.Printf("[Plugin: generic_external_review] Successfully dispatched task %s (active step: %s, form: %s)", ctx.Record.TaskID, ctx.Record.SubTaskNodeID, ctx.Record.ReviewerFormID)
	return ErrSuspended
}

func (p *ExternalReviewPlugin) Render(configRaw json.RawMessage, record store.TaskRecord, getTemplate TemplateRetriever) (map[string]any, error) {
	var cfg ExternalReviewConfig
	if len(configRaw) > 0 && string(configRaw) != "null" {
		_ = json.Unmarshal(configRaw, &cfg)
	}

	renderInfo := map[string]any{
		"form_type": "external_review",
	}

	if cfg.ReviewerJsonFormsID != "" {
		if raw, exists := getTemplate(cfg.ReviewerJsonFormsID); exists {
			var decoded map[string]any
			if err := json.Unmarshal(raw, &decoded); err == nil {
				renderInfo["reviewer_form_schema"] = decoded
			}
		}
	}
	return renderInfo, nil
}
