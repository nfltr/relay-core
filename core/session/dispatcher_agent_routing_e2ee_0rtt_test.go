// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
)

// The relay routes an agent-to-agent call's frames by transaction, never by
// type or payload: the relay client's E2EE frames (the 0-RTT first flight,
// a non-final refusal, the interactive handshake run after it on the same
// call) pass both ways unchanged, and the call stays open until an EOM.
func TestDispatcher_routeAgentToAgentLocal_E2EEFramesRoutedOpaquely(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	source := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", source, ctx)
	target := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	d.Upstreams.Store("agent-b", newTestGRPCStream("agent-b", target, ctx))
	const txnID = "txn-e2ee-0rtt"

	recv := func(ch chan *pb.TunnelFrame, wantType string, wantPayload []byte) {
		t.Helper()
		select {
		case f := <-ch:
			if f.GetMessageType() != wantType || !bytes.Equal(f.GetPayload(), wantPayload) {
				t.Fatalf("got %q %x, want %q %x", f.GetMessageType(), f.GetPayload(), wantType, wantPayload)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("no %s frame", wantType)
		}
	}
	fromSource := func(typ string, payload []byte, eom bool) {
		d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
			Payload: payload, MessageType: typ, TransactionId: txnID, Timestamps: &pb.Timestamps{},
			Routing: &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"}, Eom: eom,
		})
	}
	fromTarget := func(typ string, payload []byte, eom bool) {
		_ = d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
			Payload: payload, MessageType: typ, TransactionId: txnID, Timestamps: &pb.Timestamps{},
			Routing: &pb.RoutingInfo{AgentId: "agent-b"}, Eom: eom,
		}, fake.TestLogger())
	}
	flight := bytes.Repeat([]byte{0x01, 0xff}, 80)
	fromSource("a2a-e2ee-init-0rtt", flight, false)
	recv(target.sendCh, "a2a-e2ee-init-0rtt", flight)
	fromTarget("a2a-e2ee-retry", []byte{1}, false)
	recv(source.sendCh, "a2a-e2ee-retry", []byte{1})
	fromSource("a2a-e2ee-init", []byte("pub"), false)
	recv(target.sendCh, "a2a-e2ee-init", []byte("pub"))
	fromTarget("a2a-e2ee-ack", []byte("ack"), false)
	recv(source.sendCh, "a2a-e2ee-ack", []byte("ack"))
	fromSource("a2a-request", []byte("sealed"), true)
	recv(target.sendCh, "a2a-request", []byte("sealed"))
	fromTarget("a2a-status", nil, true)
	recv(source.sendCh, "a2a-status", nil)
}
