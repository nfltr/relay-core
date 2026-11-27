// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// countingDialer counts the onward dials it is asked for.
type countingDialer struct{ n *atomic.Int32 }

func (c countingDialer) DialRelay(context.Context, string, *slog.Logger) (*PodRelayStream, error) {
	c.n.Add(1)
	return nil, ErrPeerLinkOff
}

// TestRelayTunnelFromAPeerIsServedHereOnly: a RelayTunnel a peer replica
// forwarded (its record named this replica) whose agent is not here is
// refused NotFound, never forwarded again: a record flapping between two
// replicas, or one naming this replica with no stream here (the loopback),
// would bounce or loop the stream with no hop bound (relay-server
// spec/README.md, Replicas finding 4). TCP sessions are served the same
// way (ServeLinkTCP).
func TestRelayTunnelFromAPeerIsServedHereOnly(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()
	var dials atomic.Int32
	d.PodDialer = countingDialer{&dials}
	stream := &mockRelayTunnelServer{
		ctx:      relayPeerCtx(t, context.Background()),
		recvMsgs: []*pb.TunnelFrame{{Routing: &pb.RoutingInfo{AgentId: "owner.gone"}}},
	}
	err := d.RelayTunnel(stream)
	if status.Code(err) != codes.NotFound {
		t.Fatalf("RelayTunnel = %v, want NotFound", err)
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("forwarded onward %d times", n)
	}
}
