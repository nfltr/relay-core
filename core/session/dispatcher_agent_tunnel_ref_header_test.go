// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc/metadata"
)

// An agent is routable (Upstreams) before AgentTunnel sends its ref header.
// A frame routed to it in that window made gRPC send empty headers first:
// the client found no ref and took its plain id, so every A2A E2EE call for
// its ref was refused as a misroute (lane A2AFINAL2, the hub's warm on
// arrival). The header comes before any frame.
func TestAgentTunnelRefHeaderPrecedesARoutedFrame(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const id = "node-host"
	d.OnAgentConnect = func(agentID, _ string) { // inside the window: routable, no header yet
		s, ok := d.Upstreams.Load(agentID)
		if !ok {
			t.Error("agent not routable when OnAgentConnect runs")
			return
		}
		sent := make(chan struct{})
		go func() {
			_ = sendNewCall(s.(*GRPCStream), &pb.TunnelFrame{TransactionId: "early"})
			close(sent)
		}()
		select { // unfixed, the frame goes out now; fixed, it waits for the header
		case <-sent:
		case <-time.After(200 * time.Millisecond):
		}
	}
	tunnel, err := client.AgentTunnel(metadata.NewOutgoingContext(ctx, metadata.Pairs("x-apikey-agentID", id, "x-auth-method", "apikey")))
	if err != nil {
		t.Fatal(err)
	}
	md, err := tunnel.Header()
	if err != nil {
		t.Fatal(err)
	}
	if got := md.Get(agentwire.AgentRefKey); len(got) != 1 || got[0] != id {
		t.Fatalf("ref header %q, want %q: a frame went out before it", got, id)
	}
	for { // heartbeats aside, the routed frame arrives
		frame, err := tunnel.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if frame.GetTransactionId() == "early" {
			return
		}
	}
}
