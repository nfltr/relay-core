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

// ===========================================================================
// processGRPCInput: DropAgentOnDownstreamErr
// ===========================================================================

func TestProcessGRPCInput_DropAgentOnDownstreamErr(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		DropAgentOnDownstreamErr: true,
	})

	agentID := "agentID-drop-agent"
	agentMock := &fake.MockAgentTunnelServer{Ctx: makeGRPCContext(agentID)}
	stream := NewGRPCStream(agentID, agentMock, agentMock.Ctx, make(chan proto.Message, 10), time.NewTicker(5*time.Second))

	// Create a TunnelFrame with a transactionID that has NO registered TxnRegistry handle
	// This will make Fanout fail, and with DropAgentOnDownstreamErr=true, processGRPCInput should return error
	input := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-no-handle-drop",
		Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
	}

	err := d.processGRPCInput(ctx, stream, input, time.Now())
	if err == nil {
		t.Fatal("expected error with DropAgentOnDownstreamErr=true and Fanout failure")
	}
}

func TestProcessGRPCInput_HeartbeatChannelFullDoesNotBlock(t *testing.T) {
	t.Parallel()

	d, cancel := newTestDispatcher(t)
	defer cancel()

	agentID := "agentID-grpc-heartbeat-full"
	heartbeats := make(chan proto.Message, 1)
	heartbeats <- &pb.TunnelFrame{}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	agentMock := &fake.MockAgentTunnelServer{Ctx: makeGRPCContext(agentID)}
	stream := NewGRPCStream(agentID, agentMock, agentMock.Context(), heartbeats, ticker)

	done := make(chan error, 1)
	go func() {
		done <- d.processGRPCInput(context.Background(), stream, &pb.TunnelFrame{}, time.Now())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processGRPCInput() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("processGRPCInput() blocked on a full heartbeat channel")
	}
}

// ===========================================================================
// processHTTPInput: ctx.Done during heartbeat send
// ===========================================================================

func TestProcessHTTPInput_CtxDoneDuringHeartbeat(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, nil, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	// Create an agent stream with a full heartbeats channel (unbuffered, no reader)
	heartbeats := make(chan proto.Message) // unbuffered, will block
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	agentStream := NewHTTPStream("agentID-hb-ctx", nil, nil, heartbeats, ticker)

	// A nil Response means it's a heartbeat
	input := &pb.HTTPProxyResponse{
		Response: nil,
	}

	// Cancel the context so the select picks ctx.Done()
	hbCtx, hbCancel := context.WithCancel(context.Background())
	hbCancel()

	err := d.processHTTPInput(hbCtx, agentStream, input, time.Now())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

// ===========================================================================
// watchAgentSession: timeout with DeleteIPs error
// ===========================================================================

func TestWatchAgentSession_HeartbeatTimeoutWithDeleteIPsError(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	ctx := context.Background()

	inv := &errorIPInventory{
		deleteErr: errors.New("cache delete error"),
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, nil, inv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	heartbeats := make(chan proto.Message)
	ticker := time.NewTicker(50 * time.Millisecond) // fires quickly
	defer ticker.Stop()

	stream := &mockHeartbeatTracker{
		agentID:    "agentID-timeout-del-err",
		heartbeats: heartbeats,
		ticker:     ticker,
	}

	d.Config.HeartbeatEnabled = true
	err := d.WatchAgentSession(context.Background(), stream.AgentID(), stream, inv, nil)
	if !errors.Is(err, errNoHeartbeat) {
		t.Fatalf("expected errNoHeartbeat, got: %v", err)
	}
}

// ===========================================================================
// watchAgentSession: globalContext.Done with no shutdown window (gracefulShutdownTimeout=0)
// ===========================================================================

func TestWatchAgentSession_StopGlobalContextDone_NoWindowLeavesAtOnce(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	globalCtx, globalCancel := context.WithCancel(context.Background())

	d := NewDispatcher(globalCtx, fake.TestLogger(), b, nil, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 0, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	// Cancel global context to trigger this path
	globalCancel()

	err := d.WatchAgentSession(context.Background(), "agentID-signal", nil, nil, nil)
	if !errors.Is(err, errServerRestarting) {
		t.Fatalf("expected the agent to leave at once (server restarting), got: %v", err)
	}
}

// ===========================================================================
// watchAgentSession: globalContext.Done with successful jitter (happy path)
// ===========================================================================

func TestWatchAgentSession_StopGlobalContextDone_ServerRestarting(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	globalCtx, globalCancel := context.WithCancel(context.Background())

	d := NewDispatcher(globalCtx, fake.TestLogger(), b, nil, directory.NewNoOpInventory(), directory.NewNoOpInventory(), time.Millisecond, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	globalCancel()

	err := d.WatchAgentSession(context.Background(), "agentID-restart", nil, nil, nil)
	if !errors.Is(err, errServerRestarting) {
		t.Fatalf("expected errServerRestarting, got: %v", err)
	}
}

// ===========================================================================
// processHTTPInput: nil Metadata backward compat (L926-928)
// ===========================================================================

func TestProcessHTTPInput_NilRouting(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	agentID := "oph-nil-meta"
	ctx := t.Context()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	mockStream := &mockConnectHTTPServer{ctx: makeGRPCContext(agentID)}
	agentStream := NewHTTPStream(agentID, mockStream, nil, heartbeats, ticker)

	// Store a response channel so routeResponseToBroker() succeeds (in-memory broker)
	input := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{
			TransactionId: "txn-nil-meta",
			StatusCode:    200,
		},
		Timestamps: &pb.Timestamps{
			ResponseAtGrpcServer: time.Now().UnixMilli(),
		},
		Routing: nil, // <-- nil Routing triggers backward compat path
	}

	err := d.processHTTPInput(ctx, agentStream, input, time.Now())
	// processHTTPInput calls routeResponseToBroker() which uses in-memory broker Send.
	// The important thing is that Routing got initialized and AgentID set.
	if err != nil {
		t.Logf("processHTTPInput returned (expected for missing topic consumer): %v", err)
	}
	if input.Routing == nil {
		t.Fatal("expected Routing to be initialized")
	}
	if input.Routing.AgentId != agentID {
		t.Fatalf("expected Routing.AgentID = %s, got %s", agentID, input.Routing.AgentId)
	}
}

// ===========================================================================
// processGRPCInput: nil Routing backward compat (L980-982)
// ===========================================================================

func TestProcessGRPCInput_NilRouting(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	agentID := "oph-grpc-nil-meta"
	ctx := t.Context()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	agentMock := &fake.MockAgentTunnelServer{Ctx: makeGRPCContext(agentID)}
	stream := NewGRPCStream(agentID, agentMock, agentMock.Context(), heartbeats, ticker)

	// Register txn so Fanout succeeds
	handle, cleanup := d.TxnRegistry.Register("txn-grpc-nil-meta", ctx, fake.TestLogger())
	defer cleanup()
	// Drain the response channel so Fanout doesn't block
	go func() {
		for range handle.ResponseCh {
		}
	}()

	input := &pb.TunnelFrame{
		MessageType:   "test",
		TransactionId: "txn-grpc-nil-meta",
		Eom:           true,
		Timestamps:    &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing:       nil, // <-- nil Metadata triggers backward compat path
	}

	err := d.processGRPCInput(ctx, stream, input, time.Now())
	if err != nil {
		t.Logf("processGRPCInput returned: %v", err)
	}
	if input.Routing == nil {
		t.Fatal("expected Metadata to be initialized")
	}
	if input.Routing.AgentId != agentID {
		t.Fatalf("expected Metadata.AgentID = %s, got %s", agentID, input.Routing.AgentId)
	}
}
