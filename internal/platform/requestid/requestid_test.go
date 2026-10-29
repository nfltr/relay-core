// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package requestid

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestHandleRequestID_ExistingID(t *testing.T) {
	md := metadata.Pairs(DefaultRequestIDKey, "existing-id-123")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id := handleRequestID(ctx)
	if id != "existing-id-123" {
		t.Fatalf("expected existing-id-123, got %s", id)
	}
}

func TestHandleRequestID_DeprecatedKey(t *testing.T) {
	md := metadata.Pairs(DeprecatedRequestIDKey, "deprecated-id-456")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id := handleRequestID(ctx)
	if id != "deprecated-id-456" {
		t.Fatalf("expected deprecated-id-456, got %s", id)
	}
}

func TestHandleRequestID_GeneratesNew(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))

	id := handleRequestID(ctx)
	if id == "" {
		t.Fatal("expected generated ID, got empty string")
	}
	// UUIDs are 36 chars.
	if len(id) != 36 {
		t.Fatalf("expected UUID (36 chars), got %d chars: %s", len(id), id)
	}
}

func TestHandleRequestID_NoMetadata(t *testing.T) {
	id := handleRequestID(context.Background())
	if id == "" {
		t.Fatal("expected generated ID, got empty string")
	}
	if len(id) != 36 {
		t.Fatalf("expected UUID, got: %s", id)
	}
}

func TestHandleRequestID_PrefersDefault(t *testing.T) {
	md := metadata.Pairs(
		DefaultRequestIDKey, "default-id",
		DeprecatedRequestIDKey, "deprecated-id",
	)
	ctx := metadata.NewIncomingContext(context.Background(), md)

	id := handleRequestID(ctx)
	if id != "default-id" {
		t.Fatalf("expected default-id, got %s", id)
	}
}

func TestNewContext_SetsOutgoingMetadata(t *testing.T) {
	ctx := NewContext(context.Background(), "my-request-id")

	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	vals := md.Get(DefaultRequestIDKey)
	if len(vals) == 0 || vals[0] != "my-request-id" {
		t.Fatalf("expected my-request-id, got %v", vals)
	}
}

func TestUnaryServerInterceptor_SetsRequestID(t *testing.T) {
	interceptor := UnaryServerInterceptor()

	md := metadata.Pairs(DefaultRequestIDKey, "unary-req-id")
	ctx := metadata.NewIncomingContext(context.Background(), md)

	var outCtx context.Context
	handler := func(ctx context.Context, req any) (any, error) {
		outCtx = ctx
		return "ok", nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	outMD, ok := metadata.FromOutgoingContext(outCtx)
	if !ok {
		t.Fatal("expected outgoing metadata in handler context")
	}
	vals := outMD.Get(DefaultRequestIDKey)
	if len(vals) == 0 || vals[0] != "unary-req-id" {
		t.Fatalf("expected unary-req-id, got %v", vals)
	}
}

func TestUnaryServerInterceptor_GeneratesWhenMissing(t *testing.T) {
	interceptor := UnaryServerInterceptor()
	ctx := metadata.NewIncomingContext(context.Background(), metadata.New(nil))

	var outCtx context.Context
	handler := func(ctx context.Context, req any) (any, error) {
		outCtx = ctx
		return nil, nil
	}

	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{}, handler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	outMD, ok := metadata.FromOutgoingContext(outCtx)
	if !ok {
		t.Fatal("expected outgoing metadata")
	}
	vals := outMD.Get(DefaultRequestIDKey)
	if len(vals) == 0 || vals[0] == "" {
		t.Fatal("expected generated request ID")
	}
}

type testCtxKey struct{}

func TestWrappedStream_Context(t *testing.T) {
	ctx := context.WithValue(context.Background(), testCtxKey{}, "test")
	ws := &wrappedStream{ctx: ctx}
	if ws.Context() != ctx {
		t.Fatal("wrapped stream should return overridden context")
	}
}

func TestConstants(t *testing.T) {
	if DefaultRequestIDKey != "X-Request-ID" {
		t.Fatalf("unexpected default key: %s", DefaultRequestIDKey)
	}
	if DeprecatedRequestIDKey != "Request-Id" {
		t.Fatalf("unexpected deprecated key: %s", DeprecatedRequestIDKey)
	}
}
