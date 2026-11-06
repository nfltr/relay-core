// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"net"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// TestTrustLoopbackRefusesProxiedCalls (F-33): a loopback peer is trusted
// only when nothing says it forwards another client's call. A reverse proxy
// on the relay's host connects from loopback but adds a forwarding header,
// and a call through it must authenticate like any other.
func TestTrustLoopbackRefusesProxiedCalls(t *testing.T) {
	for _, tc := range []struct {
		name    string
		addr    string
		md      metadata.MD
		trusted bool
	}{
		{"loopback, direct", "127.0.0.1:5000", nil, true},
		{"ipv6 loopback, direct", "[::1]:5000", metadata.Pairs("x-agent-id", "w1"), true},
		{"not loopback", "192.0.2.7:5000", nil, false},
		{"x-forwarded-for", "127.0.0.1:5000", metadata.Pairs("x-forwarded-for", "198.51.100.1"), false},
		{"forwarded", "127.0.0.1:5000", metadata.Pairs("forwarded", "for=198.51.100.1"), false},
		{"x-real-ip", "127.0.0.1:5000", metadata.Pairs("x-real-ip", "198.51.100.1"), false},
	} {
		addr, err := net.ResolveTCPAddr("tcp", tc.addr)
		if err != nil {
			t.Fatal(err)
		}
		ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})
		if tc.md != nil {
			ctx = metadata.NewIncomingContext(ctx, tc.md)
		}
		var trusted bool
		_, _ = TrustLoopbackUnaryInterceptor()(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
			_, trusted = identity.FromContext(ctx)
			return nil, nil
		})
		if trusted != tc.trusted {
			t.Errorf("%s: trusted %v, want %v", tc.name, trusted, tc.trusted)
		}
	}
}
