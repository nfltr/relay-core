// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package auth reads the principal the relay's interceptors asserted in
// gRPC context metadata.
package auth

import (
	"context"
	"errors"

	"github.com/nfltr/relay-core/core/identity"
)

var errMissingToken = errors.New("unable to get token from context")

// GetAgentIDFromContext returns the AgentID the caller authenticated as:
// the principal's the auth (or the DEV ONLY trust-loopback) interceptor
// put into the context (identity.FromContext), whatever provider
// authenticated its credential (a key, a bearer token, a client
// certificate).
func GetAgentIDFromContext(ctx context.Context) (string, error) {
	if p, ok := identity.FromContext(ctx); ok && p.AgentID != "" {
		return p.AgentID, nil
	}
	return "", errMissingToken
}
