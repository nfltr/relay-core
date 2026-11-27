// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
)

// A call whose route ended before the source's last frame (its target
// offline: answered with an error) takes the source's later frames of that
// transaction as the dead call's, dropped: they never open a second call
// that starts mid-request (a truncated call to a target that came back, or
// a second error for one transaction). The source's EOM forgets the
// transaction, so its id may open a call again.
func TestA2ALateFramesDoNotReopenAnEndedCall(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	ctx := context.Background()
	src := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	source := newTestGRPCStream("agent-a", src, ctx)
	frame := func(eom bool) *pb.TunnelFrame {
		return &pb.TunnelFrame{Payload: []byte("part"), MessageType: "test", TransactionId: "txn-late",
			Timestamps: &pb.Timestamps{}, Routing: &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"}, Eom: eom}
	}
	expectError := func() {
		t.Helper()
		select {
		case m := <-src.sendCh:
			if m.GetMessageType() != msgTypeError {
				t.Fatalf("got %q, want an error answer", m.GetMessageType())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no error answer")
		}
	}
	d.dispatchAgentToAgent(ctx, source, frame(false))
	expectError()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		if _, ok := d.a2aSessions.Load("txn-late"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the route never ended")
		}
	}
	d.dispatchAgentToAgent(ctx, source, frame(false))
	d.dispatchAgentToAgent(ctx, source, frame(true))
	select {
	case m := <-src.sendCh:
		t.Fatalf("a late frame opened a second call: answered %q", m.GetMessageType())
	case <-time.After(200 * time.Millisecond):
	}
	if _, ok := d.a2aSessions.Load("txn-late"); ok {
		t.Fatal("a late frame opened a second session")
	}
	d.dispatchAgentToAgent(ctx, source, frame(true)) // after its EOM the id is new again
	expectError()
}
