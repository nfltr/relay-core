// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"slices"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// serverAssertedMetadataKeys are set by this package's interceptors from a
// verified credential (API key, fleet token, mTLS certificate) and trusted
// downstream by GetAgentIDFromContext and OPA. A client-sent copy is never
// trusted: without this, a caller with no key could send
// x-apikey-agentid and x-auth-method and register as any agent.
var serverAssertedMetadataKeys = []string{
	"x-apikey-agentid", "x-apikey-base-id", "x-auth-method", "x-mtls-agentid",
	"x-fleet-token-id", "x-fleet-selector",
}

func withoutClientAssertedIdentity(ctx context.Context) context.Context {
	// Copy only to strip: a copy is held for the stream's lifetime.
	if !slices.ContainsFunc(serverAssertedMetadataKeys, func(k string) bool { return len(metadata.ValueFromIncomingContext(ctx, k)) > 0 }) {
		return ctx
	}
	md, _ := metadata.FromIncomingContext(ctx) // a copy of its own
	for _, k := range serverAssertedMetadataKeys {
		md.Delete(k)
	}
	return metadata.NewIncomingContext(ctx, md)
}

// StripAssertedIdentityUnaryInterceptor must run before every other
// interceptor.
func StripAssertedIdentityUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(withoutClientAssertedIdentity(ctx), req)
	}
}

// StripAssertedIdentityStreamInterceptor must run before every other
// interceptor.
func StripAssertedIdentityStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: withoutClientAssertedIdentity(ss.Context())})
	}
}
