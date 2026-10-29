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
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAgentInventoryRegistrationContextPrefersLiveDispatcherContext(t *testing.T) {
	globalCtx := t.Context()
	streamCtx, streamCancel := context.WithCancel(context.Background())
	streamCancel()
	d := &Dispatcher{globalContext: globalCtx}

	got := d.agentInventoryRegistrationContext(streamCtx)
	if got != globalCtx {
		t.Fatalf("registration context = %p, want live dispatcher context %p", got, globalCtx)
	}
	if err := got.Err(); err != nil {
		t.Fatalf("registration context unexpectedly canceled: %v", err)
	}
}

func TestAgentInventoryRegistrationContextFallsBackAfterDispatcherCancel(t *testing.T) {
	globalCtx, globalCancel := context.WithCancel(context.Background())
	globalCancel()
	streamCtx := context.Background()
	d := &Dispatcher{globalContext: globalCtx}

	got := d.agentInventoryRegistrationContext(streamCtx)
	if got != streamCtx {
		t.Fatalf("registration context = %p, want stream context %p", got, streamCtx)
	}
}

func TestNewDispatcherConfig(t *testing.T) {
	viper.Set("rpc.connected_agents_threshold", 50)
	viper.Set("rpc.agent.heartbeat.recvthreshold", 30*time.Second)
	viper.Set("rpc.agent.heartbeat.sendduration", 10*time.Second)
	viper.Set("rpc.agent.heartbeat.enable", true)
	viper.Set("rpc.recv_buffer_size", 20)
	viper.Set("grpc.proxying.downstream.sync_map.clear_timeout", 2*time.Second)
	viper.Set("grpc-port", 9090)
	viper.Set("on_downstream_errors.drop_agent_connection", true)
	viper.Set("loopback", "127.0.0.1")
	viper.Set("admin.message", "maintenance")
	defer viper.Reset()

	cfg := NewDispatcherConfig()
	if cfg.ConnectedAgentsThreshold != 50 {
		t.Fatalf("expected 50, got %d", cfg.ConnectedAgentsThreshold)
	}
	if cfg.HeartbeatEnabled != true {
		t.Fatal("expected heartbeat enabled")
	}
	if !cfg.DropAgentOnDownstreamErr {
		t.Fatal("expected DropAgentOnDownstreamErr true")
	}
	if cfg.LoopbackAddr != "127.0.0.1" {
		t.Fatalf("expected loopback 127.0.0.1, got %q", cfg.LoopbackAddr)
	}
	if !cfg.AdminMessageSet {
		t.Fatal("expected admin message to be marked configured")
	}
	if cfg.AdminMessage != "maintenance" {
		t.Fatalf("expected admin message maintenance, got %q", cfg.AdminMessage)
	}
}

func TestDispatcher_IsAgentConnected(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	if d.IsAgentConnected("agentID-1") {
		t.Fatal("should not be connected")
	}

	// Store an agent
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	mock := &mockHTTPStreamer{}
	d.Agents.Store("agentID-1", NewHTTPStream("agentID-1", mock, nil, heartbeats, ticker))

	if !d.IsAgentConnected("agentID-1") {
		t.Fatal("should be connected")
	}
}

func TestDispatcher_GetAllConnectedAgents(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	agents := d.GetAllConnectedAgents()
	if len(agents) != 0 {
		t.Fatalf("expected 0 agents, got %d", len(agents))
	}

	// Store agents
	for _, agentID := range []string{"a1", "a2", "a3"} {
		heartbeats := make(chan proto.Message, 1)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		d.Agents.Store(agentID, NewHTTPStream(agentID, &mockHTTPStreamer{}, nil, heartbeats, ticker))
	}

	agents = d.GetAllConnectedAgents()
	if len(agents) != 3 {
		t.Fatalf("expected 3 agents, got %d", len(agents))
	}
}

func TestDispatcher_classifyStreamError(t *testing.T) {
	globalCtx, globalCancel := context.WithCancel(context.Background())
	d := &Dispatcher{globalContext: globalCtx}

	tests := []struct {
		name string
		err  error
		prep func()
	}{
		{"server restarting", errServerRestarting, func() {}},
		{"no heartbeat", errNoHeartbeat, func() {}},
		{"context canceled with global cancel", context.Canceled, func() { globalCancel() }},
		{"context canceled without global cancel", context.Canceled, func() {}},
		{"other error", errors.New("random"), func() {}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prep()
			result := d.classifyStreamError(tt.err)
			// All cases return either nil or a gRPC status error
			if tt.err == context.Canceled && d.globalContext.Err() == nil && result != nil {
				t.Fatal("expected nil for context.Canceled without global cancel")
			}
		})
	}
}

func TestIsExpectedStreamDisconnect(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, true},
		{"context canceled", context.Canceled, true},
		{"grpc canceled", status.Error(codes.Canceled, "context canceled"), true},
		{"grpc transport closing", status.Error(codes.Unavailable, "transport is closing"), true},
		{"wrapped grpc transport closing", errors.New("send message failed: rpc error: code = Unavailable desc = transport is closing: pod to pod stream error"), true},
		{"grpc unavailable other", status.Error(codes.Unavailable, "connection refused"), false},
		{"heartbeat timeout", errNoHeartbeat, false},
		{"other", errors.New("boom"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isExpectedStreamDisconnect(tt.err); got != tt.want {
				t.Fatalf("isExpectedStreamDisconnect() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDispatcher_validateAgentConnection_AlreadyConnected(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	mc := newMockCache()
	mc.Data["dipi:agentID:agentID-1"] = &directory.Value{PodIP: "10.0.0.1", Env: "test"}
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	_, err := d.validateAgentConnection(context.Background(), "agentID-1", "", inv)
	if err == nil {
		t.Fatal("expected error for already connected agent")
	}
}

func TestDispatcher_validateAgentConnection_StaleReconnect(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	// Simulate this pod's IP.
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.5")
	viper.Set("loopback", "127.0.0.1")

	// Redis inventory has a stale entry for agentID-1 on *this* pod.
	mc := newMockCache()
	mc.Data["dipi:agentID:agentID-1"] = &directory.Value{PodIP: "10.0.0.5", Env: "test"}
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	// No local stream exists (simulates ungraceful disconnect).
	// validateAgentConnection should clean up and allow reconnection.
	_, err := d.validateAgentConnection(context.Background(), "agentID-1", "", inv)
	if err != nil {
		t.Fatalf("stale reconnection should be allowed, got: %v", err)
	}

	// Verify the stale cache entry was cleaned up.
	if _, ok := mc.Data["dipi:agentID:agentID-1"]; ok {
		t.Fatal("expected stale inventory entry to be deleted")
	}
}

func TestDispatcher_validateAgentConnection_LiveLocalReject(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	// Simulate this pod's IP.
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.5")
	viper.Set("loopback", "127.0.0.1")

	mc := newMockCache()
	mc.Data["dipi:agentID:agentID-1"] = &directory.Value{PodIP: "10.0.0.5", Env: "test"}
	inv, _ := directory.NewIPInventory(fake.TestLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	// A live stream exists — this is a genuine duplicate, not a stale entry.
	d.Agents.Store("agentID-1", &HTTPStream{})

	_, err := d.validateAgentConnection(context.Background(), "agentID-1", "", inv)
	if err == nil {
		t.Fatal("expected error when agent has a live local stream")
	}
}

func TestDispatcher_validateAgentConnection_ThresholdReached(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	d.Config.ConnectedAgentsThreshold = 0 // 0 means unlimited; set to a specific value
	d.Config.ConnectedAgentsThreshold = 1
	d.Agents.Store("agentID-existing", &HTTPStream{ID: "agentID-existing"})

	_, err := d.validateAgentConnection(context.Background(), "agentID-new", "", directory.NewNoOpInventory())
	if err == nil {
		t.Fatal("expected error for threshold reached")
	}
}

func TestDispatcher_validateAgentConnection_Success(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	_, err := d.validateAgentConnection(context.Background(), "agentID-new", "", directory.NewNoOpInventory())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func newTestDispatcher(t *testing.T) (*Dispatcher, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)

	producer := &fake.TestProducer{}
	logger := fake.TestLogger()

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
	// Routing tests use bare agent ids of one tenant; ownership tests
	// (agent_reach_grpc_test.go) set it back to false.
	d.SingleTenant = true

	return d, cancel
}
