// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deadOwnerPodDialer resolves through the shared inventory like the
// production dialer and refuses every dial to deadIP, as a crashed relay whose
// inventory record has not expired yet does (connection refused until the
// dialer's own retries give up). Nothing reaches a handler on such a dial.
type deadOwnerPodDialer struct {
	inv     *ownerTestInventory
	deadIP  string
	inner   PodDialer
	dials   atomic.Int32
	evicted atomic.Int32
}

func (d *deadOwnerPodDialer) DialRelay(ctx context.Context, agentID string, logger *slog.Logger) (*PodRelayStream, error) {
	d.dials.Add(1)
	ip, err := d.inv.GetIP(ctx, agentID)
	if err != nil {
		return nil, status.Error(codes.NotFound, "agent not connected")
	}
	if ip == d.deadIP {
		return nil, status.Errorf(codes.Unavailable, "reached max retries for pod to pod connection: %s", ip)
	}
	res, err := d.inner.DialRelay(ctx, agentID, logger)
	if err != nil {
		return nil, err
	}
	res.cleanup = func() { d.evicted.Add(1) }
	return res, nil
}

// A peer relay's NotFound (the worker never registered there) reaches the
// source as the same typed NotFound a local route answers, not as a generic
// Internal "remote routing error", and the healthy peer's pooled connection
// is kept.
func TestDispatcher_routeAgentToAgent_PeerNotFoundStaysTyped(t *testing.T) {
	remoteClient, _, remoteCleanup := newA2ABufConnEnv(t)
	defer remoteCleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	inv := newOwnerTestInventory()
	inv.set("10.0.0.2")
	dialer := &deadOwnerPodDialer{inv: inv, inner: &relayOnlyPodDialer{client: remoteClient, ctx: ctx}}
	d, cancelSource := newTestDispatcher(t)
	defer cancelSource()
	d.Config.LoopbackAddr = "127.0.0.1"
	d.StreamerIPInventory = inv
	d.PodDialer = dialer
	withOwner(d)
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("relay-orchestrator", sourceStreamer, ctx)

	d.dispatchAgentToAgent(ctx, sourceStream, multiPodAssignFrame("ghost-worker"))
	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError || codes.Code(errMsg.GetStatusCode()) != codes.NotFound {
			t.Fatalf("got %s %v %q, want the peer's error NotFound", errMsg.GetMessageType(), codes.Code(errMsg.GetStatusCode()), errMsg.GetStatusDetail())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no answer from the peer route")
	}
	if n := dialer.evicted.Load(); n != 0 {
		t.Fatalf("peer connection evicted %d times for a typed peer answer", n)
	}
}

func TestPeerAnsweredStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{status.Error(codes.NotFound, "target agent not connected"), true},
		{status.Error(codes.PermissionDenied, "denied"), true},
		{status.Error(codes.Unavailable, errTargetAgentDisconnected.Error()), true},
		{status.Error(codes.Unavailable, "connection refused"), false},
		{status.Error(codes.Internal, "boom"), false},
		{status.Error(codes.Canceled, "Canceled"), false},
		{context.Canceled, false},
		{nil, false},
	} {
		if _, got := peerAnsweredStatus(tc.err); got != tc.want {
			t.Errorf("peerAnsweredStatus(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// Sibling hop: a RelayTunnel that forwards to the next pod passes that pod's
// typed NotFound back unchanged instead of an Internal "error: ...".
func TestDispatcher_RelayTunnel_ForwardedPeerNotFoundStaysTyped(t *testing.T) {
	nextClient, _, nextCleanup := newA2ABufConnEnv(t) // no pod dialer: unknown agent is NotFound
	defer nextCleanup()
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	d.PodDialer = &relayOnlyPodDialer{client: nextClient, ctx: ctx}

	stream, err := client.RelayTunnel(ctx)
	if err != nil {
		t.Fatalf("RelayTunnel: %v", err)
	}
	if err := stream.Send(&pb.TunnelFrame{
		Payload:       []byte(`{"type":"ping"}`),
		TransactionId: "txn-forward-not-found",
		Routing:       &pb.RoutingInfo{AgentId: "caller", TargetAgentId: "absent-agent"},
		Eom:           true,
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("Recv err = %v, want the next pod's NotFound", err)
	}
}

// ownerTestInventory is the cluster inventory a multi-pod relay routes by:
// empty until a replica records the agent at its pod IP.
type ownerTestInventory struct {
	directory.IPInventory
	mu    sync.Mutex
	podIP string
}

func newOwnerTestInventory() *ownerTestInventory {
	return &ownerTestInventory{IPInventory: directory.NewNoOpInventory()}
}

func (i *ownerTestInventory) set(podIP string) {
	i.mu.Lock()
	i.podIP = podIP
	i.mu.Unlock()
}

func (i *ownerTestInventory) GetIP(_ context.Context, agentID string) (string, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.podIP == "" {
		return "", fmt.Errorf("%w: %s", directory.ErrAgentNotConnected, agentID)
	}
	return i.podIP, nil
}

func multiPodAssignFrame(workerID string) *pb.TunnelFrame {
	return &pb.TunnelFrame{
		Payload:       []byte(`{"type":"task.assign"}`),
		MessageType:   "orchestration.v1",
		TransactionId: "txn-assign-test",
		Routing:       &pb.RoutingInfo{AgentId: "relay-orchestrator", TargetAgentId: workerID},
		Timestamps:    &pb.Timestamps{},
		Eom:           true,
	}
}
