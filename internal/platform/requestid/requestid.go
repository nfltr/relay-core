// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package requestid provides gRPC interceptors for request ID propagation.
// Package requestid provides gRPC request-ID middleware.
package requestid

import (
	"context"
	"uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// DefaultRequestIDKey is the metadata key for request IDs.
	DefaultRequestIDKey = "X-Request-ID"
	// DeprecatedRequestIDKey is an older metadata key for request IDs.
	DeprecatedRequestIDKey = "Request-Id"
)

// UnaryServerInterceptor returns a gRPC unary interceptor that ensures
// a request ID is present in the context.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		reqID := handleRequestID(ctx)
		ctx = NewContext(ctx, reqID)
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a gRPC stream interceptor that ensures
// a request ID is present in the context.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		reqID := handleRequestID(ctx)
		ctx = NewContext(ctx, reqID)
		wrapped := &wrappedStream{ServerStream: stream, ctx: ctx}
		return handler(srv, wrapped)
	}
}

// NewContext creates a new outgoing context with the request ID.
func NewContext(ctx context.Context, reqID string) context.Context {
	md := metadata.Pairs(DefaultRequestIDKey, reqID)
	return metadata.NewOutgoingContext(ctx, md)
}

// handleRequestID extracts a request ID from gRPC metadata or generates a new one.
func handleRequestID(ctx context.Context) string {
	if vals := metadata.ValueFromIncomingContext(ctx, DefaultRequestIDKey); len(vals) > 0 && vals[0] != "" {
		return vals[0]
	}
	if vals := metadata.ValueFromIncomingContext(ctx, DeprecatedRequestIDKey); len(vals) > 0 && vals[0] != "" {
		return vals[0]
	}
	return uuid.New().String()
}

// wrappedStream wraps a grpc.ServerStream with an overridden context.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
