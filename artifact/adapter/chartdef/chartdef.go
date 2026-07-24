// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Lanka Software Foundation

package chartdef

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/OpenNSW/core/artifact"
	"github.com/mushrafmim/fsm"
)

// Kind is owned here — the type that confers "artifact-ness" on an fsm chart. A
// chart is the state-machine definition a Task micro-journey runs on; it is
// stored and versioned like any other artifact.
const Kind artifact.Kind = "chart"

// loadable is the adapter: a local type (so we may legally define methods on it)
// that embeds the pure fsm.Chart and satisfies artifact.Artifact + artifact.Parser.
// Unexported — callers use Load/LoadVersion and never see it.
type loadable struct {
	fsm.Chart
}

func (loadable) Kind() artifact.Kind { return Kind }

func (c *loadable) Parse(raw []byte) error {
	if err := json.Unmarshal(raw, &c.Chart); err != nil {
		return fmt.Errorf("decode chart: %w", err)
	}
	// Reject a malformed chart at load time rather than halfway through an
	// execution — Validate enforces schema version, a reachable initial state,
	// declared terminals, and deterministic routing.
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid chart: %w", err)
	}
	return nil
}

// Load returns the newest version of a chart. Callers get a plain fsm.Chart back —
// they never see the adapter or know "artifact" was involved in fetching it.
func Load(ctx context.Context, reg *artifact.Registry, id string) (fsm.Chart, error) {
	c, err := artifact.Latest[loadable](ctx, reg, id)
	return c.Chart, err
}

// LoadVersion returns a specific pinned version (e.g. to resume a running
// instance on the version it started with).
func LoadVersion(ctx context.Context, reg *artifact.Registry, id, version string) (fsm.Chart, error) {
	c, err := artifact.Get[loadable](ctx, reg, id, version)
	return c.Chart, err
}
