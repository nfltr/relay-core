// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/keyfile"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	testAgentID  = "agent-1"
	testAgentID2 = "my-agent"
)

// ---------------------------------------------------------------------------
// validateAPIKeyFromMetadata
// ---------------------------------------------------------------------------

func TestValidateAPIKeyFromMetadata_ValidKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testAgentID, "rpc_secret123")

	md := metadata.Pairs("x-api-key", "rpc_secret123")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// AgentID should be in context value.
	agentID, ok := AgentIDFromContext(ctx)
	if !ok || agentID != testAgentID {
		t.Fatalf("expected AgentID 'agent-1', got %q (ok=%v)", agentID, ok)
	}

	// The principal is typed, never strings in metadata.
	outMD, _ := metadata.FromIncomingContext(ctx)
	if vals := outMD.Get("x-apikey-agentID"); len(vals) != 0 {
		t.Fatalf("x-apikey-agentID in metadata %v: want the principal only", vals)
	}
	if p, ok := identity.FromContext(ctx); !ok || p.Provider != keyfile.ProviderName || p.Subject != testAgentID || p.Owner != "keys:"+testAgentID {
		t.Fatalf("principal %+v, %v", p, ok)
	}
}

// TestValidateAPIKeyFromMetadata_AgentIDOverride verifies that when x-agent-id
// is present in metadata alongside a valid key, the agent-supplied name is
// scoped to the key's stored agentID to prevent global name collisions.
func TestValidateAPIKeyFromMetadata_AgentIDOverride(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add("default-agent", "rpc_secret123")

	md := metadata.Pairs("x-api-key", "rpc_secret123", "x-agent-id", "custom-name")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// The override "custom-name" should be scoped under the key's identity:
	// "default-agent.custom-name" (not bare "custom-name").
	want := "default-agent.custom-name"
	agentID, ok := AgentIDFromContext(ctx)
	if !ok || agentID != want {
		t.Fatalf("expected AgentID %q, got %q (ok=%v)", want, agentID, ok)
	}

}

// TestValidateAPIKeyFromMetadata_AgentIDSameAsKey verifies that when the
// override matches the key's stored agentID, no prefix is added.
func TestValidateAPIKeyFromMetadata_AgentIDSameAsKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testAgentID2, "rpc_secret456")

	md := metadata.Pairs("x-api-key", "rpc_secret456", "x-agent-id", testAgentID2)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	agentID, ok := AgentIDFromContext(ctx)
	if !ok || agentID != testAgentID2 {
		t.Fatalf("expected AgentID 'my-agent', got %q (ok=%v)", agentID, ok)
	}
}

// TestValidateAPIKeyFromMetadata_InvalidKey: a key no provider accepts is
// refused (fail closed), whatever stores the relay has.
func TestValidateAPIKeyFromMetadata_InvalidKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testAgentID, "rpc_secret123")

	md := metadata.Pairs("x-api-key", "rpc_wrong_key")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	if _, err := validateAPIKeyFromMetadata(ctx, store); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an unknown key: %v, want Unauthenticated", err)
	}
}

func TestValidateAPIKeyFromMetadata_NoKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testAgentID, "rpc_secret123")

	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))
	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := AgentIDFromContext(ctx)
	if ok {
		t.Fatal("expected no AgentID when no key in metadata")
	}
}

func TestValidateAPIKeyFromMetadata_NilStore(t *testing.T) {
	md := metadata.Pairs("x-api-key", "rpc_secret123")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	// No store: no provider accepts the key, refused (and no panic).
	if _, err := validateAPIKeyFromMetadata(ctx, nil); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("a key with no store: %v, want Unauthenticated", err)
	}
}

func TestValidateAPIKeyFromMetadata_NoMetadata(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testAgentID, "rpc_secret123")

	ctx := context.Background() // no metadata at all
	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, ok := AgentIDFromContext(ctx)
	if ok {
		t.Fatal("expected no AgentID with no metadata in context")
	}
}

// ---------------------------------------------------------------------------
// APIKeyUnaryInterceptor
// ---------------------------------------------------------------------------

func TestAPIKeyUnaryInterceptor_ValidKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testAgentID, "rpc_test_key")

	interceptor := APIKeyUnaryInterceptor(store)

	md := metadata.Pairs("x-api-key", "rpc_test_key")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var capturedCtx context.Context
	handler := func(ctx context.Context, req any) (any, error) {
		capturedCtx = ctx
		return "ok", nil
	}

	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}

	agentID, ok := AgentIDFromContext(capturedCtx)
	if !ok || agentID != testAgentID {
		t.Fatalf("expected AgentID 'agent-1', got %q (ok=%v)", agentID, ok)
	}
}

func TestAPIKeyUnaryInterceptor_NilStore(t *testing.T) {
	interceptor := APIKeyUnaryInterceptor(nil)

	ctx := context.Background()
	handler := func(ctx context.Context, req any) (any, error) {
		return "ok", nil
	}

	resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("expected passthrough with nil store")
	}
}

// ---------------------------------------------------------------------------
// APIKeyStreamInterceptor
// ---------------------------------------------------------------------------

type fakeServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func TestAPIKeyStreamInterceptor_ValidKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add("stream-agent", "rpc_stream_key")

	interceptor := APIKeyStreamInterceptor(store)

	md := metadata.Pairs("x-api-key", "rpc_stream_key")
	ctx := metadata.NewIncomingContext(context.Background(), md)
	ss := &fakeServerStream{ctx: ctx}

	var capturedCtx context.Context
	handler := func(srv any, stream grpc.ServerStream) error {
		capturedCtx = stream.Context()
		return nil
	}

	err := interceptor(nil, ss, &grpc.StreamServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	agentID, ok := AgentIDFromContext(capturedCtx)
	if !ok || agentID != "stream-agent" {
		t.Fatalf("expected AgentID 'stream-agent', got %q (ok=%v)", agentID, ok)
	}
}

func TestAPIKeyStreamInterceptor_NilStore(t *testing.T) {
	interceptor := APIKeyStreamInterceptor(nil)

	ctx := context.Background()
	ss := &fakeServerStream{ctx: ctx}

	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	}

	err := interceptor(nil, ss, &grpc.StreamServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("expected handler to be called with nil store")
	}
}

// TestValidateAPIKeyFromMetadata_KeyBaseID verifies that x-apikey-base-id
// metadata contains the pre-scoped key identity for A2A target resolution.
func TestValidateAPIKeyFromMetadata_KeyBaseID(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add("mykey", "rpc_base123")

	md := metadata.Pairs("x-api-key", "rpc_base123", "x-agent-id", "worker")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Scoped agentID "mykey.worker", the pre-scoped base (subject) "mykey".
	if p, _ := identity.FromContext(ctx); p.AgentID != "mykey.worker" || p.Subject != "mykey" {
		t.Fatalf("principal agent %q, subject %q; want mykey.worker, mykey", p.AgentID, p.Subject)
	}
}
