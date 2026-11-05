// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
)

// selfRecord is a directory whose every record names this replica (GetIP
// answers the dispatcher's loopback address).
type selfRecord struct{ directory.IPInventory }

func (selfRecord) GetIP(context.Context, string) (string, error) { return "127.0.0.1", nil }

// a2aArrivalEnv: a dispatcher whose records name itself, a source agent and
// a pod dialer that must not be called (a dial would be to this replica).
func a2aArrivalEnv(t *testing.T) (*Dispatcher, *GRPCStream, *channelStreamer, chan struct{}, context.Context) {
	t.Helper()
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	t.Cleanup(viper.Reset)
	d.StreamerIPInventory = selfRecord{directory.NewNoOpInventory()}
	dialed := make(chan struct{}, 1)
	d.PodDialer = &forbiddenPodDialer{called: dialed}
	ctx, cancelCtx := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancelCtx)
	src := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	return d, newTestGRPCStream("agent-a", src, ctx), src, dialed, ctx
}

func a2aCall(target string) *pb.TunnelFrame {
	return &pb.TunnelFrame{Payload: []byte("offer"), MessageType: "p2p.v1", TransactionId: "txn-" + target,
		Timestamps: &pb.Timestamps{}, Routing: &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: target}, Eom: true}
}

// The target's record names this replica while its stream here registers
// (claimAgentRecord before Upstreams.Store; spec finding 45, Move.tla
// ArriveB): the call waits for the registration and reaches it here, never
// a dial to this replica itself.
func TestA2ACallWaitsForAnArrivingTarget(t *testing.T) {
	d, src, out, dialed, ctx := a2aArrivalEnv(t)
	go func() {
		time.Sleep(100 * time.Millisecond)
		registerEchoAgent(ctx, d, "agent-b")
		d.notifyUpstreamRegistered("agent-b")
	}()
	d.dispatchAgentToAgent(ctx, src, a2aCall("agent-b"))
	select {
	case resp := <-out.sendCh:
		if string(resp.GetPayload()) != "offer" {
			t.Fatalf("answer %q (status %d %s); want the arriving target's echo", resp.GetPayload(), resp.GetStatusCode(), resp.GetStatusDetail())
		}
	case <-dialed:
		t.Fatal("dialed a replica for a target whose record names this one")
	case <-time.After(5 * time.Second):
		t.Fatal("no answer")
	}
}

// A target whose record names this replica and never registers (a stale
// record) is offline: NotFound after the wait, never a dial to itself.
func TestA2ACallToAStaleRecordOfThisReplicaIsNotDialled(t *testing.T) {
	d, src, out, dialed, ctx := a2aArrivalEnv(t)
	d.dispatchAgentToAgent(ctx, src, a2aCall("agent-gone"))
	select {
	case resp := <-out.sendCh:
		if codes.Code(resp.GetStatusCode()) != codes.NotFound { // #nosec G115 -- a gRPC code
			t.Fatalf("answer status %d %q; want NotFound", resp.GetStatusCode(), resp.GetStatusDetail())
		}
	case <-dialed:
		t.Fatal("dialed a replica for a target whose record names this one")
	case <-time.After(5 * time.Second):
		t.Fatal("no answer")
	}
}

// A RelayTunnel a peer forwarded (the record names this replica) while the
// target's stream here registers reaches it, not refused NotFound.
func TestRelayTunnelWaitsForAnArrivingAgent(t *testing.T) {
	d, _, _, dialed, ctx := a2aArrivalEnv(t)
	loop := NewInternalAgentLoopback(ctx, "agent-b")
	defer loop.Close()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cleanup := d.RegisterInternalAgent(ctx, loop.AgentID(), loop.DispatcherSide())
		<-ctx.Done()
		cleanup()
	}()
	stream := &mockRelayTunnelServer{ctx: relayPeerCtx(t, ctx),
		recvMsgs: []*pb.TunnelFrame{{Routing: &pb.RoutingInfo{AgentId: "agent-b"}, TransactionId: "txn-1", Payload: []byte("hi")}}}
	errc := make(chan error, 1)
	go func() { errc <- d.RelayTunnel(stream) }()
	got := make(chan *pb.TunnelFrame, 1)
	go func() {
		if f, err := loop.AgentSide().Recv(); err == nil {
			got <- f
		}
	}()
	select {
	case f := <-got:
		if f.GetTransactionId() != "txn-1" {
			t.Fatalf("agent got %v", f)
		}
	case err := <-errc:
		t.Fatalf("RelayTunnel = %v before reaching the arriving agent", err)
	case <-dialed:
		t.Fatal("dialed onward")
	case <-time.After(5 * time.Second):
		t.Fatal("the agent got nothing")
	}
}
