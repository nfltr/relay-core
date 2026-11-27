// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"strings"
	"testing"
	"time"

	"log/slog"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

func newDedupTestDispatcher(t *testing.T) (*Dispatcher, context.CancelFunc, *bytes.Buffer) {
	t.Helper()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)

	producer := &fake.TestProducer{}

	cfg := DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    2 * time.Second,
		HeartbeatEnabled:         false,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
		GRPCPort:                 9090,
		DropAgentOnDownstreamErr: false,
		LoopbackAddr:             "127.0.0.1",
		AdminMessageSet:          true,
	}

	d := NewDispatcher(ctx, logger, b, producer, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, cfg)
	return d, cancel, &logBuf
}

func dedupTestGRPCStream(t *testing.T, agentID string) *GRPCStream {
	t.Helper()
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	t.Cleanup(ticker.Stop)
	return NewGRPCStream(agentID, &mockStreamer{}, context.Background(), heartbeats, ticker)
}

func dedupTestFrame(agentID, txnID string, payload []byte) *pb.TunnelFrame {
	return &pb.TunnelFrame{
		Payload:       payload,
		MessageType:   "test",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
		Routing:       &pb.RoutingInfo{AgentId: agentID},
	}
}

// TestDispatcherDeliversByteIdenticalFrames: the relay is pure transport. A
// stream may legitimately repeat a payload (a 4-byte length prefix, a
// repeated chunk of a file, an identical ICE candidate frame); the wire has no
// sender sequence number, so a content hash cannot tell that from a replay.
// The relay once dropped such frames ("dropping duplicate inbound event"),
// which broke relayed p2p calls; every frame must be delivered.
func TestDispatcherDeliversByteIdenticalFrames(t *testing.T) {
	d, cancel, logBuf := newDedupTestDispatcher(t)
	defer cancel()

	stream := dedupTestGRPCStream(t, "worker_a")
	ctx := context.Background()
	payload := []byte{0, 0, 0, 253} // a p2p frame length prefix, sent again for the next frame of that size

	handle, cleanup := d.TxnRegistry.Register("txn-dedup", ctx, d.Logger)
	defer cleanup()

	got := make(chan *pb.TunnelFrame, 2)
	go func() {
		for range 2 {
			select {
			case frame := <-handle.ResponseCh:
				got <- frame
			case <-time.After(2 * time.Second):
				got <- nil
			}
		}
	}()

	frame := dedupTestFrame("worker_a", "txn-dedup", payload)
	if err := d.processGRPCInput(ctx, stream, frame, time.Now()); err != nil {
		t.Fatalf("first processGRPCInput: %v", err)
	}
	if err := d.processGRPCInput(ctx, stream, frame, time.Now()); err != nil {
		t.Fatalf("second processGRPCInput: %v", err)
	}

	var delivered int
	for range 2 {
		if f := <-got; f != nil {
			delivered++
		}
	}
	if delivered != 2 {
		t.Fatalf("fanout count = %d, want 2 (byte-identical frames are distinct data)", delivered)
	}
	if strings.Contains(logBuf.String(), "duplicate") {
		t.Fatalf("relay dropped a frame as duplicate: %s", logBuf.String())
	}
}

func TestDispatcherInboundDedup_PassesNewSeq(t *testing.T) {
	d, cancel, _ := newDedupTestDispatcher(t)
	defer cancel()

	stream := dedupTestGRPCStream(t, "worker_a")
	ctx := context.Background()

	for i, payload := range [][]byte{[]byte("payload-a"), []byte("payload-b"), []byte("payload-c")} {
		txnID := "txn-seq"
		handle, cleanup := d.TxnRegistry.Register(txnID, ctx, d.Logger)
		got := make(chan *pb.TunnelFrame, 1)
		go func() {
			select {
			case frame := <-handle.ResponseCh:
				got <- frame
			case <-time.After(2 * time.Second):
				got <- nil
			}
		}()

		if err := d.processGRPCInput(ctx, stream, dedupTestFrame("worker_a", txnID, payload), time.Now()); err != nil {
			cleanup()
			t.Fatalf("event %d processGRPCInput: %v", i, err)
		}
		if f := <-got; f == nil {
			cleanup()
			t.Fatalf("event %d: expected fanout delivery", i)
		}
		cleanup()
	}
}

func TestDispatcherInboundDedup_IndependentPerWorker(t *testing.T) {
	d, cancel, _ := newDedupTestDispatcher(t)
	defer cancel()

	ctx := context.Background()
	streamA := dedupTestGRPCStream(t, "worker_a")
	streamB := dedupTestGRPCStream(t, "worker_b")
	payload := []byte("shared-payload")

	for _, tc := range []struct {
		stream *GRPCStream
		agent  string
		txn    string
	}{
		{streamA, "worker_a", "txn-a"},
		{streamB, "worker_b", "txn-b"},
	} {
		handle, cleanup := d.TxnRegistry.Register(tc.txn, ctx, d.Logger)
		got := make(chan *pb.TunnelFrame, 1)
		go func() {
			select {
			case frame := <-handle.ResponseCh:
				got <- frame
			case <-time.After(2 * time.Second):
				got <- nil
			}
		}()

		if err := d.processGRPCInput(ctx, tc.stream, dedupTestFrame(tc.agent, tc.txn, payload), time.Now()); err != nil {
			cleanup()
			t.Fatalf("%s processGRPCInput: %v", tc.agent, err)
		}
		if f := <-got; f == nil {
			cleanup()
			t.Fatalf("%s: expected fanout for same payload from distinct worker", tc.agent)
		}
		cleanup()
	}
}
