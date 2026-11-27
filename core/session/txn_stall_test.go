// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
)

// An agent's responses to a caller that takes none of them (a stuck
// RelayTunnel caller, a stuck a2a source) must not hold the agent's receive
// loop: past the stall bound that call is refused (its consumer sees
// handle.Stalled, the agent gets a cancel) and the agent's other calls go on.
// Before, the loop waited on the stuck call until its caller went away
// (relay-link-package-2026-10-10.md "F1 fixed", sibling receiveFromGRPCAgent
// -> TxnRegistry.Fanout).
func TestGRPCAgentLoopRefusesAStuckCaller(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()
	d.TxnRegistry.Stall = 50 * time.Millisecond

	ctx := context.Background()
	agentStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 64), ctx: ctx}
	agent := newTestGRPCStream("agent-x", agentStreamer, ctx)
	stuck, cleanStuck := d.TxnRegistry.Register("txn-stuck", ctx, d.Logger)
	defer cleanStuck()
	other, cleanOther := d.TxnRegistry.Register("txn-other", ctx, d.Logger)
	defer cleanOther()

	frame := func(txnID string) *pb.TunnelFrame {
		return &pb.TunnelFrame{TransactionId: txnID, Payload: []byte("r"), Timestamps: &pb.Timestamps{}}
	}
	loop := make(chan error, 1)
	go func() {
		for range 40 {
			if err := d.processGRPCInput(ctx, agent, frame("txn-stuck"), time.Now()); err != nil {
				loop <- err
				return
			}
		}
		loop <- d.processGRPCInput(ctx, agent, frame("txn-other"), time.Now())
	}()
	select {
	case err := <-loop:
		if err != nil {
			t.Fatalf("the agent's loop ended: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a caller that takes nothing holds the agent's receive loop")
	}
	select {
	case <-other.ResponseCh:
	default:
		t.Fatal("the agent's other call got nothing")
	}
	select {
	case <-stuck.Stalled:
	default:
		t.Fatal("the stuck call's consumer was not told it was refused")
	}
	if c := waitFrame(t, agentStreamer.sendCh, "txn-stuck", "the agent's cancel"); c.GetMessageType() != msgTypeCancel || codes.Code(c.GetStatusCode()) != codes.Canceled {
		t.Fatalf("agent got %q %v for the refused call, want a cancel", c.GetMessageType(), codes.Code(c.GetStatusCode()))
	}
}
