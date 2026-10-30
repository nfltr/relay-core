// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A relay tunnel this replica forwards onward to an agent on a replica it
// cannot reach ends Unavailable with the replica link's answer, not as an
// unclassified error the forwarding replica cannot tell from a fault.
func TestRelayTunnelUnreachableReplicaIsUnavailable(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	const why = "agent bob.far is connected to another replica of the relay, which could not be reached; try again"
	d.PodDialer = &failingPodDialer{err: fmt.Errorf("%w: %w", &link.Abort{Refused: true, Code: link.CodeNoRoute, Msg: why}, errors.New("connection refused"))}
	d.ClusterRelayPeers = true
	frame := &pb.TunnelFrame{Routing: &pb.RoutingInfo{AgentId: "bob.far"}, Eom: true}
	err := d.RelayTunnel(&mockRelayTunnelServer{ctx: t.Context(), recvMsgs: []*pb.TunnelFrame{frame}})
	if st, _ := status.FromError(err); st.Code() != codes.Unavailable || st.Message() != why {
		t.Fatalf("RelayTunnel = %v, want Unavailable %q", err, why)
	}
}
