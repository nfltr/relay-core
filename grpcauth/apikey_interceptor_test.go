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

// testMachine is a key file machine bound to service "svc"; its agent is
// testRef.
var testMachine = keyfile.Machine{ID: identity.NewMachineID(), Name: "agent-1"}

var testRef = identity.AgentRef{Machine: testMachine.ID, Service: "svc"}.String()

// ---------------------------------------------------------------------------
// validateAPIKeyFromMetadata
// ---------------------------------------------------------------------------

func TestValidateAPIKeyFromMetadata_ValidKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testMachine, "rpc_secret123")

	md := metadata.Pairs("x-api-key", "rpc_secret123")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	ctx, err := validateAPIKeyFromMetadata(ctx, store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A connection naming no service is a client, not an agent.
	if agentID, ok := AgentIDFromContext(ctx); ok {
		t.Fatalf("a client got agent id %q", agentID)
	}

	// The principal is typed, never strings in metadata.
	outMD, _ := metadata.FromIncomingContext(ctx)
	if vals := outMD.Get("x-apikey-agentID"); len(vals) != 0 {
		t.Fatalf("x-apikey-agentID in metadata %v: want the principal only", vals)
	}
	if p, ok := identity.FromContext(ctx); !ok || p.Provider != keyfile.ProviderName || p.Machine != testMachine.ID || p.Owner != "keys:"+string(testMachine.ID) {
		t.Fatalf("principal %+v, %v", p, ok)
	}
}

// TestValidateAPIKeyFromMetadata_AgentIsMachineService: x-agent-id names
// a service and the agent is the key's machine's ref for it; a name that
// is no valid service (another machine's ref) is refused.
func TestValidateAPIKeyFromMetadata_AgentIsMachineService(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testMachine, "rpc_secret123")
	ctx, err := validateAPIKeyFromMetadata(metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-api-key", "rpc_secret123", "x-agent-id", "svc")), store)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agentID, ok := AgentIDFromContext(ctx); !ok || agentID != testRef {
		t.Fatalf("AgentID %q (ok=%v), want %q", agentID, ok, testRef)
	}
	if _, err := validateAPIKeyFromMetadata(metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-api-key", "rpc_secret123", "x-agent-id", identity.AgentRef{Machine: identity.MachineIDOf("other"), Service: "svc"}.String())), store); err == nil {
		t.Fatal("another machine's ref was taken")
	}
}

// TestValidateAPIKeyFromMetadata_InvalidKey: a key no provider accepts is
// refused (fail closed), whatever stores the relay has.
func TestValidateAPIKeyFromMetadata_InvalidKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testMachine, "rpc_secret123")

	md := metadata.Pairs("x-api-key", "rpc_wrong_key")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	if _, err := validateAPIKeyFromMetadata(ctx, store); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an unknown key: %v, want Unauthenticated", err)
	}
}

func TestValidateAPIKeyFromMetadata_NoKey(t *testing.T) {
	store := keyfile.NewAPIKeyStore()
	store.Add(testMachine, "rpc_secret123")

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
	store.Add(testMachine, "rpc_secret123")

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
	store.Add(testMachine, "rpc_test_key")

	interceptor := APIKeyUnaryInterceptor(store)

	md := metadata.Pairs("x-api-key", "rpc_test_key", "x-agent-id", "svc")
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
	if !ok || agentID != testRef {
		t.Fatalf("expected AgentID %q, got %q (ok=%v)", testRef, agentID, ok)
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
	store.Add(testMachine, "rpc_stream_key")

	interceptor := APIKeyStreamInterceptor(store)

	md := metadata.Pairs("x-api-key", "rpc_stream_key", "x-agent-id", "svc")
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
	if !ok || agentID != testRef {
		t.Fatalf("expected AgentID %q, got %q (ok=%v)", testRef, agentID, ok)
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
