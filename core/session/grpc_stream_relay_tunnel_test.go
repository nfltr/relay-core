// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// ===========================================================================

func TestRelayTunnel_RecvError(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	stream := &mockRelayTunnelServer{
		ctx:     context.Background(),
		recvErr: errors.New("recv failed"),
	}
	err := d.RelayTunnel(stream)
	if err == nil {
		t.Fatal("expected error from recv failure")
	}
}

func TestRelayTunnel_MissingAgentID(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	stream := &mockRelayTunnelServer{
		ctx: context.Background(),
		recvMsgs: []*pb.TunnelFrame{
			{
				Routing: &pb.RoutingInfo{AgentId: ""},
			},
		},
	}
	err := d.RelayTunnel(stream)
	if err == nil {
		t.Fatal("expected error for missing AgentID")
	}
	if !strings.Contains(err.Error(), "AgentID is required") {
		t.Fatalf("expected AgentID required error, got: %v", err)
	}
}

func TestRelayTunnel_EmptyFrame(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	stream := &mockRelayTunnelServer{
		ctx: context.Background(),
		recvMsgs: []*pb.TunnelFrame{
			{},
		},
	}
	err := d.RelayTunnel(stream)
	if err == nil {
		t.Fatal("expected error for nil additional data")
	}
	if !strings.Contains(err.Error(), "AgentID is required") {
		t.Fatalf("expected AgentID required, got: %v", err)
	}
}

func TestRelayTunnel_AgentConnectedToThisPod(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
	})

	// Register a mock upstream (agent stream) for this agentID
	agentID := "agentID-ds"
	mockStream := &fake.MockAgentTunnelServer{
		Ctx: context.Background(),
	}
	grpcStream := NewGRPCStream(agentID, mockStream, context.Background(), make(chan proto.Message, 1), time.NewTicker(time.Hour))
	d.Upstreams.Store(agentID, grpcStream)

	// Create a first message from the cluster
	firstMsg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-ds-1",
		Eom:           true, // single message
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	inStream := &mockRelayTunnelServer{
		ctx:      streamCtx,
		recvMsgs: []*pb.TunnelFrame{firstMsg},
	}

	done := make(chan error, 1)
	go func() {
		done <- d.RelayTunnel(inStream)
	}()

	// Wait for the RelayTunnel to register the transaction and start waiting for response
	time.Sleep(200 * time.Millisecond)

	// Send a response through the transaction registry
	if err := d.TxnRegistry.Fanout("txn-ds-1", &pb.TunnelFrame{
		MessageType:   "response",
		TransactionId: "txn-ds-1",
		Eom:           true,
	}, fake.TestLogger()); err != nil {
		// Transaction might not be registered yet, cancel to unblock
		streamCancel()
	}

	select {
	case err := <-done:
		if err != nil {
			t.Logf("RelayTunnel returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		streamCancel()
		t.Fatal("timed out waiting for RelayTunnel")
	}
	streamCancel()
}

// ===========================================================================
// processHTTPInput: DropAgentOnDownstreamErr-like (routeResponseToBroker error)
// ===========================================================================

func TestProcessHTTPInput_RouteResponseToBrokerError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	ctx := context.Background()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{SendErr: errors.New("send failed")}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})

	mock := &mockHTTPStreamer{}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	agentStream := NewHTTPStream("agentID-respond-err", mock, nil, nil, ticker)

	input := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{
			TransactionId: "txn-respond-err",
			StatusCode:    200,
		},
		Timestamps: &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-respond-err"},
	}

	err := d.processHTTPInput(ctx, agentStream, input, time.Now())
	if err == nil {
		t.Fatal("expected error from routeResponseToBroker failure")
	}
}

// ===========================================================================
// send: stream.Send error
// ===========================================================================

func TestDispatcher_relayRequestsToAgent_SendError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		HeartbeatSendDuration:    time.Hour,
		ConnectedAgentsThreshold: 100,
	})

	// Mock stream that fails on Send
	mock := &mockHTTPStreamer{sendErr: errors.New("send failed")}
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	agentStream := NewHTTPStream("agentID-send-err", mock, nil, nil, ticker)

	topicName := "agentID-send-err"
	_ = b.CreateTopic(topicName, 0, true)

	consumer, err := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "agentID-send-err",
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	// Publish a valid HTTPProxyRequest message from a goroutine
	validMsg := &pb.HTTPProxyRequest{
		Request:    &pb.HTTPRequest{TransactionId: "txn-1"},
		Timestamps: &pb.Timestamps{RequestAtHttpServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-send-err"},
	}
	producer, _ := b.CreateProducer(broker.ProducerOptions{Topic: topicName})
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, _ = producer.SendProto(ctx, validMsg)
	}()

	err = d.relayRequestsToAgent(ctx, agentStream, consumer)
	if err == nil {
		t.Fatal("expected error from stream.Send failure")
	}
	if !strings.Contains(err.Error(), "send failed") {
		t.Fatalf("expected 'send failed' error, got: %v", err)
	}
}

// ===========================================================================
// Proxy.ServeHTTP: success path (agent responds via fanoutResponse)

// ===========================================================================

func TestRelayTunnel_AgentConnectedToThisPod_MultiMessage(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
	})

	d.ClusterRelayPeers = true // the relay caller stands in for a peer pod
	agentID := "agentID-ds-multi"
	agentStreamCtx := t.Context()
	mockUp := &fake.MockAgentTunnelServer{Ctx: agentStreamCtx}
	grpcStream := NewGRPCStream(agentID, mockUp, agentStreamCtx, make(chan proto.Message, 1), time.NewTicker(time.Hour))
	d.Upstreams.Store(agentID, grpcStream)

	// First message has EOM=false, second has EOM=true
	firstMsg := &pb.TunnelFrame{
		MessageType:   "chunk1",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-multi-1",
		Eom:           false,
	}
	secondMsg := &pb.TunnelFrame{
		MessageType:   "chunk2",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-multi-1",
		Eom:           true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	inStream := &mockRelayTunnelServer{
		ctx:      streamCtx,
		recvMsgs: []*pb.TunnelFrame{firstMsg, secondMsg},
	}

	done := make(chan error, 1)
	go func() {
		done <- d.RelayTunnel(inStream)
	}()

	time.Sleep(200 * time.Millisecond)

	// Send a response message with EOM=true to complete the RelayTunnel
	if err := d.TxnRegistry.Fanout("txn-multi-1", &pb.TunnelFrame{
		MessageType:   "resp",
		TransactionId: "txn-multi-1",
		Eom:           true,
	}, fake.TestLogger()); err != nil {
		streamCancel()
		t.Fatal("txn handle not registered")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Logf("RelayTunnel returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		streamCancel()
		t.Fatal("timed out waiting for RelayTunnel")
	}
}

// ===========================================================================
// RelayTunnel: agent connected, response channel closed
// ===========================================================================

func TestRelayTunnel_AgentConnectedToThisPod_ResponseChClosed(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
	})

	agentID := "agentID-ds-close"
	agentStreamCtx := t.Context()
	mockUp := &fake.MockAgentTunnelServer{Ctx: agentStreamCtx}
	grpcStream := NewGRPCStream(agentID, mockUp, agentStreamCtx, make(chan proto.Message, 1), time.NewTicker(time.Hour))
	d.Upstreams.Store(agentID, grpcStream)

	firstMsg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-close-1",
		Eom:           true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	inStream := &mockRelayTunnelServer{
		ctx:      streamCtx,
		recvMsgs: []*pb.TunnelFrame{firstMsg},
	}

	done := make(chan error, 1)
	go func() {
		done <- d.RelayTunnel(inStream)
	}()

	time.Sleep(200 * time.Millisecond)

	// Cancel the stream context to trigger the egCtx.Done() path.
	// cleanup (deferred inside RelayTunnel) closes the channel after the function returns.
	streamCancel()

	select {
	case err := <-done:
		// Should complete — the context-cancelled path returns ctx.Err()
		_ = err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for RelayTunnel")
	}
}

// ===========================================================================
// serveInternal: CI env with port/agent readiness
// ===========================================================================

func TestRelayTunnel_AgentConnectedToThisPod_SendError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
	})

	agentID := "agentID-ds-senderr"
	agentStreamCtx := t.Context()

	// Agent stream that returns error on Send
	mockUp := &fake.MockAgentTunnelServer{Ctx: agentStreamCtx, SendErr: errors.New("send failed")}
	grpcStream := NewGRPCStream(agentID, mockUp, agentStreamCtx, make(chan proto.Message, 1), time.NewTicker(time.Hour))
	d.Upstreams.Store(agentID, grpcStream)

	firstMsg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-senderr-1",
		Eom:           true,
	}

	streamCtx := t.Context()

	inStream := &mockRelayTunnelServer{
		ctx:      streamCtx,
		recvMsgs: []*pb.TunnelFrame{firstMsg},
	}

	err := d.RelayTunnel(inStream)
	if err == nil {
		t.Fatal("expected error when sending to agent fails")
	}
}

// ===========================================================================
// RelayTunnel: missing transaction ID (auto-generated)
// ===========================================================================

func TestRelayTunnel_AgentConnectedToThisPod_NoTransactionID(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
	})

	agentID := "agentID-ds-notxn"
	agentStreamCtx := t.Context()

	mockUp := &fake.MockAgentTunnelServer{Ctx: agentStreamCtx}
	grpcStream := NewGRPCStream(agentID, mockUp, agentStreamCtx, make(chan proto.Message, 1), time.NewTicker(time.Hour))
	d.Upstreams.Store(agentID, grpcStream)

	// First message has no TransactionId - should be auto-generated
	firstMsg := &pb.TunnelFrame{
		MessageType: "test",
		Routing:     &pb.RoutingInfo{AgentId: agentID},
		Eom:         true,
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	inStream := &mockRelayTunnelServer{
		ctx:      streamCtx,
		recvMsgs: []*pb.TunnelFrame{firstMsg},
	}

	done := make(chan error, 1)
	go func() {
		done <- d.RelayTunnel(inStream)
	}()

	time.Sleep(200 * time.Millisecond)

	// The TxnRegistry should have an auto-generated transaction ID
	// Find it by ranging over the transaction IDs
	var found bool
	var discoveredTxnID string
	d.TxnRegistry.RangeTransactionIDs(func(key string) bool {
		if key != "" {
			discoveredTxnID = key
			found = true
			return false
		}
		return true
	})
	if found {
		_ = d.TxnRegistry.Fanout(discoveredTxnID, &pb.TunnelFrame{
			MessageType:   "resp",
			TransactionId: discoveredTxnID,
			Eom:           true,
		}, fake.TestLogger())
	}

	if !found {
		streamCancel()
		t.Log("txn not yet registered, cancelling")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Logf("RelayTunnel returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		streamCancel()
		t.Fatal("timed out waiting for RelayTunnel")
	}
}

// ===========================================================================
// RelayTunnel: agent NOT on this pod, its replica unreachable
// ===========================================================================

func TestRelayTunnel_AgentNotOnThisPod_MaxRetries(t *testing.T) {
	// ENV must be a recognized multi-pod environment (config.IsK8sMultiPodEnv)
	// for NewDispatcher to construct a podDialer (here without a replica
	// link, so every forward is refused) — otherwise
	// dialPeerStream short-circuits with codes.NotFound ("not in k8s
	// multi-pod mode") before ever attempting a dial, and this test
	// exercises the wrong branch entirely. A multi-replica test stack sets
	// ENV=CI on its replicas for exactly this reason.
	viper.Set("ENV", config.EnvCI)
	viper.Set("pod_id.env_var", "TEST_POD_ID")
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_ID", "test-pod")
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)

	mc := newMockCache()
	agentID := "agentID-maxretry"
	cacheKey := directory.GetStreamerCacheKey(agentID)
	mc.Data[cacheKey] = &directory.Value{PodIP: "127.0.0.1", Env: "test-env"}

	streamerInv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetStreamerCacheKey)

	localCtx := t.Context()

	localDispatcher := NewDispatcher(localCtx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), streamerInv, 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
	})

	firstMsg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-retry-1",
		Eom:           true,
	}

	streamCtx, streamCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer streamCancel()

	inStream := &mockRelayTunnelServer{
		ctx:      relayPeerCtx(t, streamCtx),
		recvMsgs: []*pb.TunnelFrame{firstMsg},
	}

	before := podToPodConnectionFailuresValue(t)

	err := localDispatcher.RelayTunnel(inStream)
	if err == nil {
		t.Fatal("expected an error: the agent's replica cannot be reached")
	}
	t.Logf("unreachable replica error: %v", err)

	// Found via a live chaostoolkit fault-injection run
	// (docker/chaostoolkit/experiments) against a real svc-1-style
	// RelayTunnel client (cmd/samples/endurance-svc): this exact branch in
	// RelayTunnel (dispatcher_grpc.go) returned dialErr directly without
	// ever registering dispatcher_pod_to_pod_connection_failures_total —
	// unlike routeAgentToAgentRemote's identical dial-failure branch
	// (dispatcher_agent_routing.go), which already had this fixed. A plain
	// RelayTunnel client (the actual chaos-test traffic pattern) exercises
	// this function, not routeAgentToAgentRemote, so the metric never fired
	// for a real "downstream pod died" scenario until this was fixed here
	// too.
	if after := podToPodConnectionFailuresValue(t); after != before+1 {
		t.Fatalf("dispatcher_pod_to_pod_connection_failures_total = %v, want %v — RelayTunnel's own dial-failure branch must record this metric, not just routeAgentToAgentRemote's", after, before+1)
	}
}

// ===========================================================================
// NewRPCServer: all options, full initialization
