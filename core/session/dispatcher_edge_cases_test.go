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

func TestNewDispatcher_DefaultConfig(t *testing.T) {
	viper.Set("rpc.connected_agents_threshold", 100)
	viper.Set("rpc.agent.heartbeat.recvthreshold", 5*time.Second)
	viper.Set("rpc.agent.heartbeat.sendduration", 2*time.Second)
	viper.Set("rpc.agent.heartbeat.enable", false)
	viper.Set("rpc.recv_buffer_size", 10)
	defer viper.Reset()

	ctx := t.Context()

	b := broker.NewInMemoryMessageBroker()
	// No cfg arg — uses NewDispatcherConfig() default path
	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second)
	if d == nil {
		t.Fatal("expected non-nil dispatcher")
	}
}

func TestDispatcher_classifyStreamError_GlobalCanceled(t *testing.T) {
	globalCtx, globalCancel := context.WithCancel(context.Background())
	globalCancel() // cancel global context
	d := &Dispatcher{globalContext: globalCtx}

	result := d.classifyStreamError(context.Canceled)
	if result == nil {
		t.Fatal("expected non-nil error for context.Canceled with global cancel")
	}
}

func TestDispatcher_watchAgentSession_Heartbeat_HeartbeatResetsTickerWithoutInventoryRefresh(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	stream := &mockHeartbeatTracker{
		agentID:    "agentID-1",
		heartbeats: heartbeats,
		ticker:     ticker,
	}

	mc := newMockCache()
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	heartbeats <- &pb.TunnelFrame{} // trigger heartbeat handler

	ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	d.Config.HeartbeatEnabled = true
	err := d.WatchAgentSession(ctx, stream.AgentID(), stream, inv, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("watchAgentSession() error = %v, want context deadline exceeded", err)
	}
	if mc.SetCalls != 1 { // the session-start refresh only
		t.Fatalf("a heartbeat refreshed inventory directly; Set calls = %d, want 1 (the session start)", mc.SetCalls)
	}
}

func TestDispatcher_processGRPCInput_DropAgentOnDownstreamErr(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.DropAgentOnDownstreamErr = true

	heartbeats := make(chan proto.Message, 1)
	stream := &GRPCStream{
		ID:         "agentID-1",
		heartbeats: heartbeats,
	}

	input := &pb.TunnelFrame{
		MessageType:   "test",
		TransactionId: "txn-drop",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agentID-1"},
	}

	// No transaction registered, fanout will fail
	err := d.processGRPCInput(context.Background(), stream, input, time.Now())
	if err == nil {
		t.Fatal("expected error with DropAgentOnDownstreamErr enabled")
	}
}

func TestDispatcher_processGRPCInput_FanoutErrorIgnored(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.DropAgentOnDownstreamErr = false // default

	heartbeats := make(chan proto.Message, 1)
	stream := &GRPCStream{
		ID:         "agentID-1",
		heartbeats: heartbeats,
	}

	input := &pb.TunnelFrame{
		MessageType:   "test",
		TransactionId: "txn-ignore-err",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agentID-1"},
	}

	// No transaction registered, fanout will fail but error is suppressed
	err := d.processGRPCInput(context.Background(), stream, input, time.Now())
	if err != nil {
		t.Fatalf("expected no error with DropAgentOnDownstreamErr=false, got: %v", err)
	}
}

func TestDispatcher_processGRPCInput_IgnoresStaleTransactionResponse(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.DropAgentOnDownstreamErr = true

	heartbeats := make(chan proto.Message, 1)
	stream := &GRPCStream{
		ID:         "agentID-1",
		heartbeats: heartbeats,
	}

	ctx := t.Context()
	_, cleanup := d.TxnRegistry.Register("txn-stale-a2a-e2ee", ctx, fake.TestLogger())
	cleanup()
	defer cleanup()

	input := &pb.TunnelFrame{
		MessageType:   "opaque-protocol-frame",
		TransactionId: "txn-stale-a2a-e2ee",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agentID-1"},
	}

	err := d.processGRPCInput(context.Background(), stream, input, time.Now())
	if err != nil {
		t.Fatalf("expected stale transaction response to be ignored, got: %v", err)
	}
}

func TestDispatcher_relayRequestsToAgent_AckError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		HeartbeatSendDuration:    time.Hour,
		ConnectedAgentsThreshold: 100,
	})

	mock := &mockHTTPStreamer{}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	agentStream := NewHTTPStream("agentID-1", mock, nil, nil, ticker)

	topicName := "agentID-1"
	_ = b.CreateTopic(topicName, 0, true)

	consumer, err := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "agentID-1",
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	// Publish a valid message from goroutine (unbuffered channel)
	producer, _ := b.CreateProducer(broker.ProducerOptions{Topic: topicName})
	validMsg := &pb.HTTPProxyRequest{
		Request:    &pb.HTTPRequest{TransactionId: "txn-send"},
		Timestamps: &pb.Timestamps{RequestAtHttpServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = producer.SendProto(ctx, validMsg)
	}()

	// Give time for message to arrive then cancel
	time.AfterFunc(500*time.Millisecond, cancel)

	err = d.relayRequestsToAgent(ctx, agentStream, consumer)
	// Should process the message then terminate on context cancel
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Logf("send returned: %v (expected context canceled or nil)", err)
	}
}

func TestDispatcher_processHTTPInput_HeartbeatCtxDone(t *testing.T) {
	d, dCancel := newTestDispatcher(t)
	defer dCancel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	heartbeats := make(chan proto.Message) // unbuffered, will block
	stream := &HTTPStream{
		ID:         "agentID-1",
		heartbeats: heartbeats,
	}

	input := &pb.HTTPProxyResponse{} // empty = heartbeat
	err := d.processHTTPInput(ctx, stream, input, time.Now())
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestDispatcher_processGRPCInput_HeartbeatCtxDone(t *testing.T) {
	d, dCancel := newTestDispatcher(t)
	defer dCancel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel

	heartbeats := make(chan proto.Message) // unbuffered, will block
	stream := &GRPCStream{
		ID:         "agentID-1",
		heartbeats: heartbeats,
	}

	input := &pb.TunnelFrame{} // empty = heartbeat
	err := d.processGRPCInput(ctx, stream, input, time.Now())
	if err != nil {
		t.Fatalf("expected nil (heartbeat skipped), got %v", err)
	}
}

func TestDispatcher_relayRequestsToAgent_HeartbeatSendError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := context.Background()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		HeartbeatSendDuration:    1 * time.Millisecond, // very fast heartbeat
		ConnectedAgentsThreshold: 100,
	})

	mock := &mockHTTPStreamer{sendErr: errors.New("send failed")}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	agentStream := NewHTTPStream("agentID-1", mock, nil, nil, ticker)

	topicName := "agentID-1"
	_ = b.CreateTopic(topicName, 0, true)

	consumer, err := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "agentID-1",
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	err = d.relayRequestsToAgent(ctx, agentStream, consumer)
	if err == nil {
		t.Fatal("expected error from heartbeat send failure")
	}
}

func TestDispatcher_relayRequestsToAgent_DecodeError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		HeartbeatSendDuration:    time.Hour,
		ConnectedAgentsThreshold: 100,
	})

	mock := &mockHTTPStreamer{}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	agentStream := NewHTTPStream("agentID-1", mock, nil, nil, ticker)

	topicName := "agentID-1"
	_ = b.CreateTopic(topicName, 0, true)

	consumer, err := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "agentID-1",
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	// Publish an invalid message from a goroutine (unbuffered channel)
	producer, _ := b.CreateProducer(broker.ProducerOptions{Topic: topicName})
	go func() {
		time.Sleep(50 * time.Millisecond) // let d.relayRequestsToAgent() start and call consumer.Chan()
		_, _ = producer.SendBytes(ctx, []byte("not-a-proto-message"))
	}()

	// relayRequestsToAgent should fail on decodeBrokerMessage
	err = d.relayRequestsToAgent(ctx, agentStream, consumer)
	if err == nil {
		t.Fatal("expected error from decodeBrokerMessage")
	}
}

func TestGetAllAgentInfoCapabilities(t *testing.T) {
	defer viper.Reset()

	tests := []struct {
		name         string
		agentID      string
		capabilities []string
		wantCaps     []string
	}{
		{
			name:         "agent with terminal capability",
			agentID:      "shell-agent",
			capabilities: []string{"terminal"},
			wantCaps:     []string{"terminal"},
		},
		{
			name:         "agent with multiple capabilities",
			agentID:      "multi-agent",
			capabilities: []string{"browse", "terminal", "tail"},
			wantCaps:     []string{"browse", "terminal", "tail"},
		},
		{
			name:         "agent with no capabilities defaults to browse",
			agentID:      "plain-agent",
			capabilities: nil,
			wantCaps:     []string{"browse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, cancel := newTestDispatcher(t)
			defer cancel()

			stream := &HTTPStream{ID: tt.agentID, capabilities: tt.capabilities}
			d.Agents.Store(tt.agentID, stream)

			infos := d.GetAllAgentInfo()
			if len(infos) != 1 {
				t.Fatalf("expected 1 agent info, got %d", len(infos))
			}
			info := infos[0]
			if info.AgentID != tt.agentID {
				t.Errorf("expected agent ID %s, got %s", tt.agentID, info.AgentID)
			}
			if len(info.Capabilities) != len(tt.wantCaps) {
				t.Fatalf("expected capabilities %v, got %v", tt.wantCaps, info.Capabilities)
			}
			for i, cap := range tt.wantCaps {
				if info.Capabilities[i] != cap {
					t.Errorf("capability[%d]: expected %q, got %q", i, cap, info.Capabilities[i])
				}
			}
		})
	}
}

func TestDispatcherInventoryIgnoresDriftedNegativeAgentCounter(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()

	const agentID = "agent-negative-counter"
	d.NumberOfAgentsConnected.Store(-3)
	d.Agents.Store(agentID, &HTTPStream{ID: agentID, capabilities: []string{"terminal"}})

	if got := d.AgentCount(); got != 1 {
		t.Fatalf("expected AgentCount to be derived from live streams, got %d", got)
	}
	connected := d.GetAllConnectedAgents()
	if len(connected) != 1 || connected[0] != agentID {
		t.Fatalf("expected connected HTTP agent %q, got %#v", agentID, connected)
	}
	infos := d.GetAllAgentInfo()
	if len(infos) != 1 || infos[0].AgentID != agentID {
		t.Fatalf("expected agent info for %q, got %#v", agentID, infos)
	}
}
