// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
)

// stuckStreamer is an agent whose stream takes nothing: Send blocks until
// the test ends.
type stuckStreamer struct {
	release chan struct{}
	ctx     context.Context
}

func (s *stuckStreamer) Send(*pb.TunnelFrame) error { <-s.release; return errors.New("released") }
func (s *stuckStreamer) Recv() (*pb.TunnelFrame, error) {
	<-s.release
	return nil, errors.New("released")
}
func (s *stuckStreamer) Context() context.Context { return s.ctx }

func a2aFrame(txnID, source, target string, eom bool) *pb.TunnelFrame {
	return &pb.TunnelFrame{
		Payload:       []byte(dispatcherRoutingRequestPayload),
		MessageType:   "test",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: source, TargetAgentId: target},
		Eom:           eom,
	}
}

// newStuckTarget registers agent id as a connected agent that takes nothing.
func newStuckTarget(t *testing.T, d *Dispatcher, id string) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	ctx := context.Background()
	d.Upstreams.Store(id, newTestGRPCStream(id, &stuckStreamer{release: release, ctx: ctx}, ctx))
}

// waitFrame returns the first frame of txnID sent on ch within a bound.
func waitFrame(t *testing.T, ch <-chan *pb.TunnelFrame, txnID, what string) *pb.TunnelFrame {
	t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case f := <-ch:
			if f.GetTransactionId() == txnID {
				return f
			}
		case <-timeout:
			t.Fatalf("%s: no frame of %s", what, txnID)
			return nil
		}
	}
}

// A call to a target that takes nothing must not hold the source agent's
// receive loop: the loop hands the call's frames off, waits no longer than
// the stall bound, refuses that one call (ResourceExhausted to the source)
// and goes on to the source's other calls. Before, the 18th frame of the
// stuck call blocked the loop for good (relay-link-package-2026-10-10.md
// "F1 fixed", sibling dispatcher_agent_routing.go).
func TestA2AStuckTargetDoesNotStallSourceLoop(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()
	d.handoffStall = 50 * time.Millisecond

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 64), ctx: ctx}
	source := newTestGRPCStream("agent-a", sourceStreamer, ctx)
	newStuckTarget(t, d, "agent-b")
	healthy := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 64), ctx: ctx}
	d.Upstreams.Store("agent-c", newTestGRPCStream("agent-c", healthy, ctx))

	loop := make(chan struct{})
	go func() {
		defer close(loop)
		for range 2*a2aSessionBufferCap + 2 {
			d.dispatchAgentToAgent(ctx, source, a2aFrame("txn-stuck", "agent-a", "agent-b", false))
		}
		d.dispatchAgentToAgent(ctx, source, a2aFrame("txn-other", "agent-a", "agent-c", true))
	}()
	select {
	case <-loop:
	case <-time.After(5 * time.Second):
		t.Fatal("a target that takes nothing holds the source agent's receive loop")
	}
	waitFrame(t, healthy.sendCh, "txn-other", "the source's other call")
	refusal := waitFrame(t, sourceStreamer.sendCh, "txn-stuck", "the stuck call's refusal")
	if codes.Code(refusal.GetStatusCode()) != codes.ResourceExhausted || refusal.GetMessageType() != msgTypeError || !refusal.GetEom() {
		t.Fatalf("stuck call answered %v %q (eom %v), want a ResourceExhausted error", codes.Code(refusal.GetStatusCode()), refusal.GetMessageType(), refusal.GetEom())
	}
}

// A frame after a call's EOM, or another stream's frame with the call's
// transaction ID, is dropped: the session's channel is closed by its own
// source only, and nothing sends on it after (before: a send on a closed
// channel, a relay panic).
func TestA2AFrameAfterEOMIsDropped(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()
	d.handoffStall = 50 * time.Millisecond

	ctx := context.Background()
	source := newTestGRPCStream("agent-a", &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 64), ctx: ctx}, ctx)
	other := newTestGRPCStream("agent-z", &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 64), ctx: ctx}, ctx)
	newStuckTarget(t, d, "agent-b") // keeps the session open after its EOM

	d.dispatchAgentToAgent(ctx, source, a2aFrame("txn-eom", "agent-a", "agent-b", true))
	d.dispatchAgentToAgent(ctx, source, a2aFrame("txn-eom", "agent-a", "agent-b", false))
	d.dispatchAgentToAgent(ctx, source, a2aFrame("txn-eom", "agent-a", "agent-b", true))
	d.dispatchAgentToAgent(ctx, other, a2aFrame("txn-eom", "agent-z", "agent-b", true))
	d.dispatchAgentToAgent(ctx, other, &pb.TunnelFrame{MessageType: msgTypeCancel, TransactionId: "txn-eom", Routing: &pb.RoutingInfo{TargetAgentId: "agent-b"}})

	session, ok := d.a2aSessions.Load("txn-eom")
	if !ok {
		t.Fatal("the call's session ended: another stream's cancel reached it")
	}
	select {
	case <-session.done:
		t.Fatal("another stream's cancel cancelled the call")
	default:
	}
}
