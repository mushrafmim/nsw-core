// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 Lanka Software Foundation

package auth

import (
	"encoding/json"
	"fmt"
)

// authConfig is implemented by each strategy's options struct. build resolves any
// secret references the strategy declares and constructs the authenticator.
type authConfig interface {
	build() (Authenticator, error)
}

// Build constructs an authenticator for the given auth type from its raw JSON
// options. It is the single entry point callers (e.g. the remote Manager) use, so
// they need not know about individual strategies. Secret references are resolved
// here, once; an unresolvable reference is a loud error.
func Build(authType string, options json.RawMessage) (Authenticator, error) {
	var cfg authConfig
	switch authType {
	case "api_key":
		cfg = &APIKeyConfig{}
	case "bearer":
		cfg = &BearerConfig{}
	case "oauth2":
		cfg = &OAuth2Config{}
	default:
		return nil, fmt.Errorf("unsupported auth type: %q", authType)
	}

	if err := json.Unmarshal(options, cfg); err != nil {
		return nil, fmt.Errorf("invalid %s options: %w", authType, err)
	}
	return cfg.build()
}
