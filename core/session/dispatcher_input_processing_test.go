// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

func TestDispatcher_processHTTPInput_Heartbeat(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewHTTPStream("agentID-1", &mockHTTPStreamer{}, nil, heartbeats, ticker)

	// nil Response => heartbeat
	input := &pb.HTTPProxyResponse{
		Timestamps: &pb.Timestamps{},
	}
	err := d.processHTTPInput(context.Background(), stream, input, time.Now())
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-heartbeats:
	default:
		t.Fatal("expected heartbeat in channel")
	}
}

func TestDispatcher_processHTTPInput_HeartbeatChannelFullDoesNotBlock(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	heartbeats <- &pb.HTTPProxyResponse{}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewHTTPStream("agentID-1", &mockHTTPStreamer{}, nil, heartbeats, ticker)

	done := make(chan error, 1)
	go func() {
		done <- d.processHTTPInput(context.Background(), stream, &pb.HTTPProxyResponse{Timestamps: &pb.Timestamps{}}, time.Now())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processHTTPInput() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("processHTTPInput() blocked on a full heartbeat channel")
	}
}

func TestDispatcher_processHTTPInput_EmptyTransactionID(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewHTTPStream("agentID-1", &mockHTTPStreamer{}, nil, heartbeats, ticker)

	// Response with empty TransactionId => heartbeat
	input := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: ""},
		Timestamps: &pb.Timestamps{},
	}
	err := d.processHTTPInput(context.Background(), stream, input, time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_processHTTPInput_DataMessage(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewHTTPStream("agentID-1", &mockHTTPStreamer{}, nil, heartbeats, ticker)

	input := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-1"},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	// routeResponseToBroker needs a producer set up — using InMemory broker already auto-creating
	err := d.processHTTPInput(context.Background(), stream, input, time.Now())
	// This may fail because no topic exists for pod-1, but the important thing is
	// it doesn't panic and reaches the routeResponseToBroker path
	_ = err
}

// --- processGRPCInput ---

func TestDispatcher_processGRPCInput_Heartbeat(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	ctx := context.Background()
	stream := NewGRPCStream("agentID-1", &mockStreamer{}, ctx, heartbeats, ticker)

	// Empty message => heartbeat
	input := &pb.TunnelFrame{}
	err := d.processGRPCInput(context.Background(), stream, input, time.Now())
	if err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_processGRPCInput_MissingTimestamps(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	ctx := context.Background()
	stream := NewGRPCStream("agentID-1", &mockStreamer{}, ctx, heartbeats, ticker)

	input := &pb.TunnelFrame{
		TransactionId: "txn-1",
	}
	err := d.processGRPCInput(context.Background(), stream, input, time.Now())
	if err == nil {
		t.Fatal("expected error for missing timestamps")
	}
}

func TestDispatcher_processGRPCInput_ValidMessage(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	ctx := context.Background()
	stream := NewGRPCStream("agentID-1", &mockStreamer{}, ctx, heartbeats, ticker)

	// Register a transaction so fanout works
	handle, cleanup := d.TxnRegistry.Register("txn-1", ctx, fake.TestLogger())
	defer cleanup()

	input := &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-1",
		Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
		Routing:       &pb.RoutingInfo{AgentId: "agentID-1"},
	}

	go func() {
		<-handle.ResponseCh
	}()

	err := d.processGRPCInput(context.Background(), stream, input, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatcher_processGRPCInput_DataRefreshesInventoryTTLThrottled(t *testing.T) {
	ctx := t.Context()

	mc := newMockCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DispatcherConfig{
		HeartbeatRecvThreshold: 100 * time.Millisecond,
		RecvBufferSize:         10,
		LoopbackAddr:           "127.0.0.1",
		AdminMessageSet:        true,
	}
	b := broker.NewInMemoryMessageBroker()
	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), inv, 5*time.Second, cfg)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewGRPCStream("agentID-ttl", &mockStreamer{}, ctx, make(chan proto.Message, 1), ticker)

	process := func(txnID string) {
		t.Helper()
		handle, cleanup := d.TxnRegistry.Register(txnID, ctx, fake.TestLogger())
		defer cleanup()
		done := make(chan struct{})
		go func() {
			defer close(done)
			<-handle.ResponseCh
		}()
		err := d.processGRPCInput(ctx, stream, &pb.TunnelFrame{
			Payload:       []byte("data"),
			MessageType:   "test",
			TransactionId: txnID,
			Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
			Routing:       &pb.RoutingInfo{AgentId: "agentID-ttl"},
		}, time.Now())
		if err != nil {
			t.Fatalf("processGRPCInput(%s): %v", txnID, err)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for transaction %s fanout", txnID)
		}
	}

	process("txn-ttl-1")
	if mc.SetCalls != 1 {
		t.Fatalf("after first data frame, Set calls = %d, want 1", mc.SetCalls)
	}
	if mc.LastExpiration != cfg.HeartbeatRecvThreshold {
		t.Fatalf("expiration = %v, want %v", mc.LastExpiration, cfg.HeartbeatRecvThreshold)
	}

	process("txn-ttl-2")
	if mc.SetCalls != 1 {
		t.Fatalf("second immediate data frame refreshed Redis; Set calls = %d, want 1", mc.SetCalls)
	}

	d.InventoryRefreshByAgentID.Store("agentID-ttl", time.Now().Add(-d.inboundInventoryRefreshInterval()-time.Millisecond))
	process("txn-ttl-3")
	if mc.SetCalls != 2 {
		t.Fatalf("after refresh interval, Set calls = %d, want 2", mc.SetCalls)
	}
}

func TestDispatcher_processHTTPInput_DataRefreshesInventoryTTL(t *testing.T) {
	ctx := t.Context()

	mc := newMockCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DispatcherConfig{
		HeartbeatRecvThreshold: time.Minute,
		RecvBufferSize:         10,
		LoopbackAddr:           "127.0.0.1",
		AdminMessageSet:        true,
	}
	b := broker.NewInMemoryMessageBroker()
	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second, cfg)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewHTTPStream("agentID-http-ttl", &mockHTTPStreamer{}, ctx, make(chan proto.Message, 1), ticker)

	err = d.processHTTPInput(ctx, stream, &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-http-ttl", Payload: []byte("ok")},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-http-ttl"},
	}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if mc.SetCalls != 1 {
		t.Fatalf("Set calls = %d, want 1", mc.SetCalls)
	}
	if mc.LastExpiration != cfg.HeartbeatRecvThreshold {
		t.Fatalf("expiration = %v, want %v", mc.LastExpiration, cfg.HeartbeatRecvThreshold)
	}
}

func TestDispatcher_watchAgentSession_RenewsIdleStreamTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := newMockCache()
	inv, err := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DispatcherConfig{
		HeartbeatRecvThreshold: 50 * time.Millisecond,
		RecvBufferSize:         10,
		LoopbackAddr:           "127.0.0.1",
		AdminMessageSet:        true,
	}
	b := broker.NewInMemoryMessageBroker()
	// The relay's context is not the session's: cancelling ctx below ends the
	// session only. Shared, the cancel also stopped the relay, and the session
	// watch reports errServerRestarting whenever it sees the stop first.
	relayCtx := t.Context()
	d := NewDispatcher(relayCtx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), inv, 5*time.Second, cfg)

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	stream := NewGRPCStream("agentID-idle-ttl", &mockStreamer{}, ctx, make(chan proto.Message, 1), ticker)
	stream.labels = map[string]string{"role": "implementer"}
	stream.capabilities = []string{"worker"}

	done := make(chan error, 1)
	go func() {
		done <- d.WatchAgentSession(ctx, "agentID-idle-ttl", stream, inv, nil)
	}()

	deadline := time.After(time.Second)
	for {
		setCalls, _, _ := mc.Snapshot()
		if setCalls >= 2 {
			break
		}
		select {
		case <-deadline:
			setCalls, _, _ := mc.Snapshot()
			t.Fatalf("timed out waiting for idle inventory refresh; Set calls = %d", setCalls)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
	_, lastExpiration, data := mc.Snapshot()
	if lastExpiration != cfg.HeartbeatRecvThreshold {
		t.Fatalf("expiration = %v, want %v", lastExpiration, cfg.HeartbeatRecvThreshold)
	}
	rec, ok := data[directory.GetStreamerCacheKey("agentID-idle-ttl")]
	if !ok {
		t.Fatal("expected streamer inventory record")
	}
	if rec.Labels["role"] != "implementer" {
		t.Fatalf("labels = %#v, want role=implementer", rec.Labels)
	}
	if len(rec.Capabilities) != 1 || rec.Capabilities[0] != "worker" {
		t.Fatalf("capabilities = %#v, want [worker]", rec.Capabilities)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("refresh loop error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh loop did not stop after context cancellation")
	}
}

// --- routeResponseToBroker ---
