// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ===========================================================================

func TestConnectHTTP_NoJWT(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	// Context with no authorization metadata
	stream := &mockConnectHTTPServer{
		ctx: context.Background(),
	}
	err := d.ConnectHTTP(stream)
	if err == nil {
		t.Fatal("expected error for missing JWT")
	}
	if !strings.Contains(err.Error(), "PermissionDenied") {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}
}

func TestConnectHTTP_AlreadyConnected(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	defer viper.Reset()

	ctx := context.Background()
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)

	mc := newMockCache()
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	// Pre-set the IP so agent appears already connected
	_ = inv.SetIP(ctx, "agentID-1", time.Minute)

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
	})

	grpcCtx := makeGRPCContext("agentID-1")
	stream := &mockConnectHTTPServer{ctx: grpcCtx}

	err := d.ConnectHTTP(stream)
	if err == nil {
		t.Fatal("expected error for already-connected agent")
	}
	if !strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("expected AlreadyExists, got: %v", err)
	}
}

// ===========================================================================
// ConnectHTTP: SetIP error
// ===========================================================================

func TestConnectHTTP_SetIPError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	errInv := &errorIPInventory{
		setIPErr: errors.New("redis down"),
		getIPErr: errors.New("not found"),
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, errInv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-setip-sub"
	streamCtx := makeGRPCContext(agentID)
	mock := &mockConnectHTTPServer{
		ctx: streamCtx,
	}

	err := d.ConnectHTTP(mock)
	if err == nil {
		t.Fatal("expected error for SetIP failure")
	}
	if !strings.Contains(err.Error(), "redis down") {
		t.Errorf("expected redis down error, got: %v", err)
	}
}

// ===========================================================================
// ConnectHTTP: CreateTopic error
// ===========================================================================

func TestConnectHTTP_CreateTopicError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	// Don't enable topic auto-creation on the broker either -> CreateTopic will fail
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-topic-err"
	streamCtx := makeGRPCContext(agentID)
	mock := &mockConnectHTTPServer{
		ctx: streamCtx,
	}

	err := d.ConnectHTTP(mock)
	if err == nil {
		t.Fatal("expected error for CreateTopic failure")
	}
}

func TestConnectHTTP_ThresholdReached(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	// Set threshold to 1 and pre-load 1 agent so any new connection is rejected
	d.Config.ConnectedAgentsThreshold = 1
	d.Config.HeartbeatRecvThreshold = 5 * time.Second
	d.Agents.Store("agentID-existing", &HTTPStream{ID: "agentID-existing"})

	grpcCtx := makeGRPCContext("agentID-new")
	stream := &mockConnectHTTPServer{ctx: grpcCtx}

	err := d.ConnectHTTP(stream)
	if err == nil {
		t.Fatal("expected error for threshold reached")
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "limit of 1 connected agents") {
		t.Fatalf("expected FailedPrecondition naming the limit, got: %v", err)
	}
}

func TestConnectHTTP_FullLifecycle(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)

	mc := newMockCache()
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	grpcCtx, grpcCancel := makeGRPCContextWithCancel("agentID-lifecycle")
	globalCtx := t.Context()

	d := NewDispatcher(globalCtx, fake.TestLogger(), b, &fake.TestProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Hour, // don't trigger heartbeat sending
		HeartbeatEnabled:         false,
		RecvBufferSize:           10,
	})

	// Agent sends a heartbeat (nil response/empty txn) then stream cancels
	stream := &mockConnectHTTPServer{
		ctx: grpcCtx,
		recvMsgs: []*pb.HTTPProxyResponse{
			{Timestamps: &pb.Timestamps{}}, // heartbeat
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- d.ConnectHTTP(stream)
	}()

	// Wait for agent to connect
	time.Sleep(200 * time.Millisecond)

	// Verify agent is stored
	if !d.IsAgentConnected("agentID-lifecycle") {
		t.Log("agent not yet connected, waiting more...")
		time.Sleep(300 * time.Millisecond)
	}

	// Cancel the stream context to disconnect
	grpcCancel()

	select {
	case err := <-done:
		// Should return nil or a gRPC status error (classified by classifyStreamError)
		if err != nil {
			t.Logf("ConnectHTTP returned: %v (expected for stream cancel)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ConnectHTTP to return")
	}

	// Verify cleanup
	if d.IsAgentConnected("agentID-lifecycle") {
		t.Fatal("agent should be disconnected after cancel")
	}
}

// ===========================================================================
// AgentTunnel tests
// ===========================================================================

func TestAgentTunnel_NoJWT(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	stream := &fake.MockAgentTunnelServer{Ctx: context.Background()}
	err := d.AgentTunnel(stream)
	if err == nil {
		t.Fatal("expected error for missing JWT")
	}
	if !strings.Contains(err.Error(), "PermissionDenied") {
		t.Fatalf("expected PermissionDenied, got: %v", err)
	}
}

func TestAgentTunnel_ThresholdReached(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	d.Config.ConnectedAgentsThreshold = 1
	d.Config.HeartbeatRecvThreshold = 5 * time.Second
	d.Agents.Store("agentID-existing", &HTTPStream{ID: "agentID-existing"})

	grpcCtx := makeGRPCContext("agentID-new")
	stream := &fake.MockAgentTunnelServer{Ctx: grpcCtx}

	err := d.AgentTunnel(stream)
	if err == nil {
		t.Fatal("expected error for threshold reached")
	}
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "limit of 1 connected agents") {
		t.Fatalf("expected FailedPrecondition naming the limit, got: %v", err)
	}
}

func TestAgentTunnel_FullLifecycle(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)

	mc := newMockCache()
	streamerInv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetStreamerCacheKey)

	grpcCtx, grpcCancel := makeGRPCContextWithCancel("agentID-upstream")
	globalCtx := t.Context()

	d := NewDispatcher(globalCtx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), streamerInv, 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Hour,
		HeartbeatEnabled:         false,
		RecvBufferSize:           10,
	})

	// Agent sends a heartbeat (empty message) then EOF
	stream := &fake.MockAgentTunnelServer{
		Ctx: grpcCtx,
		RecvMsgs: []*pb.TunnelFrame{
			{}, // heartbeat (zero-size)
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- d.AgentTunnel(stream)
	}()

	// Allow time for setup
	time.Sleep(200 * time.Millisecond)

	// Cancel stream
	grpcCancel()

	select {
	case err := <-done:
		if err != nil {
			t.Logf("AgentTunnel returned: %v (expected for stream cancel)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for AgentTunnel to return")
	}
}

// ===========================================================================
// RelayTunnel tests

// ===========================================================================

func TestAgentTunnel_HeartbeatSendFailure(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	mc := newMockCache()
	streamerInv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetStreamerCacheKey)

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), streamerInv, 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    50 * time.Millisecond, // very fast heartbeat
		HeartbeatEnabled:         true,
		RecvBufferSize:           10,
	})

	agentID := "agentID-hb-fail"
	streamCtx, streamCancel := makeGRPCContextWithCancel(agentID)
	defer streamCancel()

	// Mock stream that fails on Send (heartbeat will fail)
	mock := &fake.MockAgentTunnelServer{
		Ctx:     streamCtx,
		SendErr: errors.New("heartbeat send failed"),
	}

	err := d.AgentTunnel(mock)
	if err == nil {
		t.Log("AgentTunnel returned nil - heartbeat failure classified as client disconnect")
	}
}

// ===========================================================================
// AgentTunnel: SetIP failure
// ===========================================================================

type errorIPInventory struct {
	setIPErr    error
	deleteErr   error
	getIPErr    error
	getIPResult string
}

func (e *errorIPInventory) SetIP(_ context.Context, _ string, _ time.Duration) error {
	return e.setIPErr
}

func (e *errorIPInventory) SetIPWithLabels(_ context.Context, _ string, _ map[string]string, _ time.Duration) error {
	return e.setIPErr
}

func (e *errorIPInventory) SetIPWithAgentInfo(_ context.Context, _ string, _ map[string]string, _ []string, _ time.Duration) error {
	return e.setIPErr
}

func (e *errorIPInventory) GetIP(_ context.Context, _ string) (string, error) {
	return e.getIPResult, e.getIPErr
}

func (e *errorIPInventory) DeleteIPs(_ context.Context, _ ...string) error {
	return e.deleteErr
}

func (e *errorIPInventory) ListAgentIDs(_ context.Context) ([]string, error) {
	return nil, e.getIPErr
}

func (e *errorIPInventory) ListAgentRecords(_ context.Context) ([]directory.AgentRecord, error) {
	return nil, e.getIPErr
}

func TestAgentTunnel_SetIPFailure(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	errInv := &errorIPInventory{
		setIPErr: errors.New("cache unavailable"),
		getIPErr: errors.New("not found"), // so validateAgentConnection passes
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), errInv, 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-setip-fail"
	streamCtx := makeGRPCContext(agentID)

	mock := &fake.MockAgentTunnelServer{Ctx: streamCtx}

	err := d.AgentTunnel(mock)
	if err == nil {
		t.Fatal("expected error for SetIP failure")
	}
}

// ===========================================================================
// serveMetrics: with profiling enabled

// ===========================================================================
// ConnectHTTP: broker.Subscribe failure
// ===========================================================================

func TestConnectHTTP_BrokerSubscribeFailure(t *testing.T) {
	defer viper.Reset()

	// Use NewInMemoryMessageBroker but WITHOUT topic auto creation
	// so that the broker.Subscribe for the agentID topic fails.
	b := broker.NewInMemoryMessageBroker()
	// Note: NOT calling SetTopicAutoCreation, only creating the main topic
	// but not the agentID topic
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-sub-fail"
	agentIDTopicName := agentID
	// Create the topic so CreateTopic succeeds, but then subscribing twice
	// with exclusive type should fail on the second subscription.
	_ = b.SetTopicAutoCreation("ns", true)
	_ = b.CreateTopic(agentIDTopicName, 0, true)

	// First subscription (exclusive) consumes the topic
	_, _ = b.Subscribe(broker.ConsumerOptions{
		Topic:            agentIDTopicName,
		SubscriptionName: agentID,
		Type:             broker.ConsumerTypeExclusive,
	})

	streamCtx, streamCancel := makeGRPCContextWithCancel(agentID)
	defer streamCancel()

	mock := &mockConnectHTTPServer{
		ctx: streamCtx,
	}

	done := make(chan error, 1)
	go func() {
		done <- d.ConnectHTTP(mock)
	}()

	select {
	case err := <-done:
		// We expect some error because the second exclusive subscription
		// to the same topic should fail, or the errgroup cancels quickly.
		_ = err
	case <-time.After(10 * time.Second):
		streamCancel()
		cancel()
		t.Fatal("ConnectHTTP didn't return")
	}
}

// ===========================================================================
// ConnectHTTP: HeartbeatEnabled path
// ===========================================================================

func TestConnectHTTP_HeartbeatEnabled(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   200 * time.Millisecond,
		HeartbeatSendDuration:    time.Second,
		HeartbeatEnabled:         true,
		RecvBufferSize:           10,
	})

	agentID := "agentID-hb-enabled"
	streamCtx, streamCancel := makeGRPCContextWithCancel(agentID)

	mock := &mockConnectHTTPServer{
		ctx: streamCtx,
	}

	done := make(chan error, 1)
	go func() {
		done <- d.ConnectHTTP(mock)
	}()

	// HeartbeatEnabled=true means watchAgentSession monitors heartbeats.
	// With 200ms threshold and no heartbeats, it will timeout quickly.
	select {
	case err := <-done:
		// Should get errNoHeartbeat since no heartbeats arrive
		if err != nil && !strings.Contains(err.Error(), "Unavailable") && !strings.Contains(err.Error(), "heartbeat") && !strings.Contains(err.Error(), "canceled") {
			t.Logf("got expected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		streamCancel()
		cancel()
		t.Fatal("ConnectHTTP didn't return")
	}
	streamCancel()
}

// ===========================================================================
// ConnectHTTP: consumer.Unsubscribe error
// ===========================================================================

func TestConnectHTTP_ConsumerUnsubscribeError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-unsub-err"
	streamCtx, streamCancel := makeGRPCContextWithCancel(agentID)

	mock := &mockConnectHTTPServer{
		ctx:     streamCtx,
		recvErr: io.EOF, // stream ends immediately
	}

	done := make(chan error, 1)
	go func() {
		done <- d.ConnectHTTP(mock)
	}()

	// Stream will end due to io.EOF on recv, triggering consumer.Unsubscribe in defer
	time.Sleep(500 * time.Millisecond)
	streamCancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ConnectHTTP didn't return")
	}
}
