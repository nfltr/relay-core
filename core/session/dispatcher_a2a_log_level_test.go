// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
)

// TestA2ARoundTripSuccessLogsOnlyAtDebug guards the relay journal volume: a
// successful agent-to-agent round trip (request from A, response from B
// through the real processGRPCInput paths) writes nothing at the production
// default level (info). Per-request success logs belong at Debug; errors,
// refusals and lifecycle events stay at Info/Warn.
func TestA2ARoundTripSuccessLogsOnlyAtDebug(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	var sink fake.LockedBuffer
	d.Logger = slog.New(slog.NewJSONHandler(&sink, &slog.HandlerOptions{Level: slog.LevelDebug}))

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	const txnID = "txn-log-level-1"
	go func() {
		<-targetStreamer.sendCh
		_ = d.processGRPCInput(ctx, targetStream, &pb.TunnelFrame{
			Payload:       []byte("pong"),
			MessageType:   "test",
			TransactionId: txnID,
			Timestamps:    &pb.Timestamps{},
			Eom:           true,
		}, time.Now())
	}()

	if err := d.processGRPCInput(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("ping"),
		MessageType:   "test",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b", DeadlineMs: time.Now().Add(time.Minute).UnixMilli()},
		Eom:           true,
	}, time.Now()); err != nil {
		t.Fatalf("processGRPCInput: %v", err)
	}
	select {
	case resp := <-sourceStreamer.sendCh:
		if string(resp.GetPayload()) != "pong" {
			t.Fatalf("response payload = %q, want pong", resp.GetPayload())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for the A2A response")
	}
	// The route goroutine finishes (and logs) after relaying the response;
	// its last line is the transaction cleanup.
	deadline := time.Now().Add(3 * time.Second)
	for !sink.Contains("cleaned up transaction") {
		if time.Now().After(deadline) {
			t.Fatal("A2A route did not clean up its transaction")
		}
		time.Sleep(5 * time.Millisecond)
	}

	counts := sink.Levels(t)
	t.Logf("A2A round trip log lines by level: %v", counts)
	if n := counts["INFO"] + counts["WARN"] + counts["ERROR"]; n != 0 {
		t.Fatalf("successful A2A round trip logged %d lines at info or above, want 0", n)
	}
	if counts["DEBUG"] == 0 {
		t.Fatal("no debug lines: the round trip did not run through the logged path")
	}
}
