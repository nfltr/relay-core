// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"context"

	"github.com/nfltr/relay-core/platform/buildinfo"
	"github.com/nfltr/relay-core/platform/clientgen"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// relayErrorDomain is the ErrorInfo domain of every relay refusal.
const relayErrorDomain = "relay.nfltr.xyz"

// helloRequired is the protocol v1 hard cut: every call carries a Hello
// (agentwire.HelloKey) or is refused before authentication, so a client
// released before v1 is told to update rather than failing on whatever the
// new relay no longer reads. A call of a client below minGeneration (agentwire.GenerationKey,
// security.min_client_generation: clientgen) is refused the same way, as
// cheaply: nothing is looked up first. An accepted call gets the relay's
// Welcome as a response header.
func helloRequired(ctx context.Context, minGeneration int) error {
	c, err := hello(ctx)
	if err != nil {
		metrics.RecordProtocolRefusal()
		return refusal(agentwire.ClientTooOld)
	}
	if !clientgen.Admit(minGeneration, agentwire.ParseGeneration(metadata.ValueFromIncomingContext(ctx, agentwire.GenerationKey)), "grpc", c.Name+" "+c.Version) {
		return refusal(agentwire.ClientGenerationTooOld)
	}
	return nil
}

// hello parses the Hello in ctx's request metadata from that one value, not
// from a copy of the whole metadata (TestReadsIncomingMetadataByKey). Not
// inlined: its map stays out of the frames of the interceptors that call
// it, which stay on a stream's stack for the stream's life (a larger frame
// there grew each idle agent's stack by 8 KiB).
//
//go:noinline
func hello(ctx context.Context) (agentwire.Client, error) {
	return agentwire.ParseHello(map[string][]string{agentwire.HelloKey: metadata.ValueFromIncomingContext(ctx, agentwire.HelloKey)})
}

// refusal is a permanent refusal of a client too old for this relay.
func refusal(msg string) error {
	st := status.New(codes.FailedPrecondition, msg)
	if detailed, derr := st.WithDetails(&errdetails.ErrorInfo{Reason: relayv1.ErrorReason_ERROR_REASON_PROTOCOL_UNSUPPORTED.String(), Domain: relayErrorDomain}); derr == nil {
		st = detailed
	}
	return st.Err()
}

// welcome is the relay's Welcome header. allow:wrapper
func welcome() metadata.MD { return metadata.Pairs(agentwire.WelcomePairs(buildinfo.Version)...) }

// ProtocolOptions are the server options of the protocol v1 handshake
// (helloRequired, the Welcome header) refusing clients below
// minGeneration (0: none), first of every server's interceptor chain:
// NewGRPCServer's, and a test server's that serves v1 clients.
func ProtocolOptions(minGeneration int) []grpc.ServerOption {
	stream := func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := helloRequired(ss.Context(), minGeneration); err != nil {
			return err
		}
		if err := ss.SetHeader(welcome()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
	unary := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := helloRequired(ctx, minGeneration); err != nil {
			return nil, err
		}
		if err := grpc.SetHeader(ctx, welcome()); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(unary), grpc.ChainStreamInterceptor(stream)}
}

// clientStreamsInterceptor counts each open stream by the client its Hello
// names (metrics.TrackClientStream), so an operator sees which client
// versions are still connected.
func clientStreamsInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	c, _ := hello(ss.Context())
	defer metrics.TrackClientStream(info.FullMethod, c.Name, c.Version)()
	return handler(srv, ss)
}
