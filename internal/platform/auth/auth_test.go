// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package auth

import (
	"context"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"google.golang.org/grpc/metadata"
)

// ---------------------------------------------------------------------------
// GetAgentIDFromContext
// ---------------------------------------------------------------------------

func TestGetAgentIDFromContext_APIKey(t *testing.T) {
	// The key interceptor's principal takes priority over everything else;
	// a client-sent x-apikey-agentID string counts for nothing.
	md := metadata.Pairs(
		"x-apikey-agentID", "forged-agent",
		"x-mtls-agentID", "mtls-agent",
	)
	ctx := identity.WithPrincipal(metadata.NewIncomingContext(context.Background(), md), identity.Principal{AgentID: "apikey-agent"})

	agentID, err := GetAgentIDFromContext(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agentID != "apikey-agent" {
		t.Fatalf("expected 'apikey-agent', got %q", agentID)
	}
}

func TestGetAgentIDFromContext_MetadataNamesNobody(t *testing.T) {
	// A client-sent x-mtls-agentID (the CN header of the deleted mTLS
	// interceptor) names nobody: certificates authenticate through the
	// provider chain, as a principal.
	md := metadata.Pairs("x-mtls-agentID", "mtls-agent")
	if _, err := GetAgentIDFromContext(metadata.NewIncomingContext(context.Background(), md)); err == nil {
		t.Fatal("x-mtls-agentID metadata named an agent")
	}
}

func TestGetAgentIDFromContext_JWT_Removed(t *testing.T) {
	// A bearer JWT in metadata is not itself an identity: only the auth
	// interceptor's principal is (the OIDC provider verifies tokens).
	md := metadata.Pairs("authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhZ2VudF9pZCI6Imp3dC1hZ2VudCJ9.c2ln")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	_, err := GetAgentIDFromContext(ctx)
	if err == nil {
		t.Fatal("expected error: JWT auth should no longer be accepted")
	}
}

func TestGetAgentIDFromContext_NoAuth(t *testing.T) {
	ctx := context.Background()
	_, err := GetAgentIDFromContext(ctx)
	if err == nil {
		t.Fatal("expected error when no auth is present")
	}
}
