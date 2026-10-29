// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package security — gRPC loopback-trust interceptor (DEV ONLY).
//
// When security.trust_loopback is enabled, a gRPC request whose peer is a
// loopback address is trusted as a fixed local principal ("local-dev") with no
// credential: the interceptor puts a principal (provider "loopback", one local
// owner) into the context as the key interceptor would, so downstream
// handlers (and GetAgentIDFromContext) resolve an identity without a key. This is the gRPC
// counterpart to the relay's HTTP loopback-trust path; together they let workers
// and the operator run `nfltr orch` locally with zero auth setup.
//
// Loopback-only (the peer address is the kernel-reported TCP peer, not a
// spoofable header), so production (non-loopback ingress) is unaffected. Off by
// default; installed only when security.trust_loopback is set.
package grpcauth

import (
	"context"
	"net"
	"strings"

	"github.com/nfltr/relay-core/core/identity"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// grpcLoopbackTrustPrincipal is the fixed agent id assigned to loopback gRPC
// peers under security.trust_loopback (mirrors the relay HTTP path).
const grpcLoopbackTrustPrincipal = "local-dev"

func grpcPeerIsLoopback(ctx context.Context) bool {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return false
	}
	host := p.Addr.String()
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func withLoopbackAgentID(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	// On loopback we trust the peer's self-declared identity (x-agent-id, the
	// same header the real API-key interceptor honors) so each worker keeps its
	// own distinct agent id; fall back to the fixed local principal only when no
	// id is declared (e.g. the operator/planner client).
	agentID := grpcLoopbackTrustPrincipal
	if ids := md.Get("x-agent-id"); len(ids) > 0 && strings.TrimSpace(ids[0]) != "" {
		agentID = strings.TrimSpace(ids[0])
	}
	return identity.WithPrincipal(ctx, identity.Principal{Provider: "loopback", Owner: "loopback:local", AgentID: agentID})
}

// TrustLoopbackUnaryInterceptor injects the local principal for loopback peers.
func TrustLoopbackUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if grpcPeerIsLoopback(ctx) {
			ctx = withLoopbackAgentID(ctx)
		}
		return handler(ctx, req)
	}
}

// TrustLoopbackStreamInterceptor injects the local principal for loopback peers.
func TrustLoopbackStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if grpcPeerIsLoopback(ss.Context()) {
			return handler(srv, &loopbackServerStream{ServerStream: ss, ctx: withLoopbackAgentID(ss.Context())})
		}
		return handler(srv, ss)
	}
}

type loopbackServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *loopbackServerStream) Context() context.Context { return s.ctx }
