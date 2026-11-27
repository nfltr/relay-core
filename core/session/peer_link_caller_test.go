// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// relayPeerCtx is the context of a RelayTunnel stream a peer replica opened
// on the replica link (core/replicas.ServeLinkRelay).
func relayPeerCtx(_ *testing.T, parent context.Context) context.Context {
	return context.WithValue(parent, PeerLinkCaller{}, true)
}

// A relay tunnel comes from a replica link peer only: any other caller,
// with a key, the peer secret as a bearer, or nothing, is refused before
// any routing.
func TestRelayTunnelPodModeRefusesUnauthenticatedCaller(t *testing.T) {
	d := &Dispatcher{PodDialer: staticRefusingDialer{}}
	t.Setenv("S2S_TOKEN", "s3cret")
	for name, ctx := range map[string]context.Context{
		"no credential": context.Background(),
		"peer secret":   metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer s3cret")),
		"agent key":     makeGRPCContext("alice.hub"),
	} {
		if err := d.authorizeRelayTunnelCaller(ctx); status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: %v; want PermissionDenied", name, err)
		}
	}
	if err := d.authorizeRelayTunnelCaller(relayPeerCtx(t, context.Background())); err != nil {
		t.Fatalf("relay peer refused: %v", err)
	}
}

// staticRefusingDialer marks a multi-replica relay; it forwards nothing.
type staticRefusingDialer struct{}

func (staticRefusingDialer) DialRelay(context.Context, string, *slog.Logger) (*PodRelayStream, error) {
	return nil, ErrPeerLinkOff
}
