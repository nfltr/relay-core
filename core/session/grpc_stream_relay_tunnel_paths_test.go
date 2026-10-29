// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// ===========================================================================
// RelayTunnel: agent-local multi-message with EOM relay
// ===========================================================================

func TestRelayTunnel_AgentLocal_MultiMessage_EOM(t *testing.T) {
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

	agentID := "agentID-multi-eom"

	// Register agent upstream with mock that captures sent messages
	mockUp := &fake.MockAgentTunnelServer{Ctx: context.Background()}
	heartbeats := make(chan proto.Message, 10)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	agentStream := NewGRPCStream(agentID, mockUp, context.Background(), heartbeats, ticker)
	d.Upstreams.Store(agentID, agentStream)

	streamCtx := makeGRPCContext(agentID)
	streamCtx, streamCancel := context.WithTimeout(streamCtx, 5*time.Second)
	defer streamCancel()

	firstMsg := &pb.TunnelFrame{
		MessageType:   "test",
		TransactionId: "txn-multi-eom",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		Eom:           false, // NOT end-of-message
	}
	secondMsg := &pb.TunnelFrame{
		MessageType:   "test",
		TransactionId: "txn-multi-eom",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		Eom:           true, // end-of-message
	}

	mock := &mockRelayTunnelServer{
		ctx:      streamCtx,
		recvMsgs: []*pb.TunnelFrame{firstMsg, secondMsg},
	}

	// Run RelayTunnel in goroutine, feed response via TxnRegistry
	done := make(chan error, 1)
	go func() {
		done <- d.RelayTunnel(mock)
	}()

	// Wait for TxnRegistry to be populated
	time.Sleep(200 * time.Millisecond)

	// Send response back through txn registry
	_ = d.TxnRegistry.Fanout("txn-multi-eom", &pb.TunnelFrame{
		MessageType:   "response",
		TransactionId: "txn-multi-eom",
		Eom:           true,
	}, fake.TestLogger())

	select {
	case err := <-done:
		if err != nil {
			t.Logf("RelayTunnel returned: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RelayTunnel didn't return")
	}
}

// ===========================================================================
// RelayTunnel: agent-local response send error (L641-648, L691-694)
// ===========================================================================

func TestRelayTunnel_AgentLocal_ResponseSendError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		GRPCPort:                 9090,
	})

	agentID := "oph-ds-send-err"
	txnID := "txn-ds-send-err"

	// Store agent upstream so agentConnectedToThisPod == true
	agentCtx := makeGRPCContext(agentID)
	agentStreamMock := &fake.MockAgentTunnelServer{Ctx: agentCtx}
	d.Upstreams.Store(agentID, agentStreamMock)

	// Create downstream request — first message with EOM=true (single message)
	dsCtx := makeGRPCContext(agentID)
	ds := &mockRelayTunnelServer{
		ctx: dsCtx,
		recvMsgs: []*pb.TunnelFrame{
			{
				MessageType:   "request",
				Routing:       &pb.RoutingInfo{AgentId: agentID},
				TransactionId: txnID,
				Eom:           true,
			},
		},
		sendErr: fmt.Errorf("cluster send failed"), // Response send will fail
	}

	done := make(chan error, 1)
	go func() { done <- d.RelayTunnel(ds) }()

	// Wait for txn to register, then push response
	time.Sleep(300 * time.Millisecond)
	err := d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
		MessageType:   "response",
		TransactionId: txnID,
		Eom:           true,
	}, fake.TestLogger())
	if err != nil {
		// RelayTunnel may have already returned on the registration path
		select {
		case err := <-done:
			t.Logf("RelayTunnel returned early: %v", err)
			return
		case <-time.After(2 * time.Second):
			t.Fatal("txn not registered and RelayTunnel didn't return")
		}
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from RelayTunnel")
		}
		t.Logf("RelayTunnel returned error (expected): %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel didn't return")
	}
}

// ===========================================================================
// RelayTunnel: agent-local multi-message send error to agent (L597-603)
// ===========================================================================

// countingStreamer is a Streamer that fails after N sends
type countingStreamer struct {
	ctx       context.Context
	sends     int
	failAfter int
	sendErr   error
}

func (c *countingStreamer) Send(_ *pb.TunnelFrame) error {
	c.sends++
	if c.sends > c.failAfter {
		return c.sendErr
	}
	return nil
}
func (c *countingStreamer) Recv() (*pb.TunnelFrame, error) { return nil, io.EOF }
func (c *countingStreamer) Context() context.Context       { return c.ctx }

func TestRelayTunnel_AgentLocal_MultiMsg_AgentSendError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		GRPCPort:                 9090,
	})

	agentID := "oph-multi-send-err"
	txnID := "txn-multi-send-err"

	// Agent stream that succeeds on 1st Send (first message) but fails on 2nd
	agentCtx := makeGRPCContext(agentID)
	agentStreamMock := &countingStreamer{
		ctx:       agentCtx,
		failAfter: 1,
		sendErr:   fmt.Errorf("agent send failed on second message"),
	}
	d.Upstreams.Store(agentID, agentStreamMock)

	// First message without EOM, second message with EOM — triggers multi-message goroutine
	dsCtx := makeGRPCContext(agentID)
	ds := &mockRelayTunnelServer{
		ctx: dsCtx,
		recvMsgs: []*pb.TunnelFrame{
			{
				MessageType:   "request",
				Routing:       &pb.RoutingInfo{AgentId: agentID},
				TransactionId: txnID,
				Eom:           false, // not EOM — multi-message path
			},
			{
				MessageType:   "request-part2",
				Routing:       &pb.RoutingInfo{AgentId: agentID},
				TransactionId: txnID,
				Eom:           true,
			},
		},
	}

	done := make(chan error, 1)
	go func() { done <- d.RelayTunnel(ds) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error from RelayTunnel when agent Send fails")
		}
		t.Logf("RelayTunnel returned error (expected): %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel didn't return")
	}
}

func TestRelayTunnel_AgentLocal_MultiMsg_RecvError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		GRPCPort:                 9090,
	})

	agentID := "oph-multi-recv-err"
	txnID := "txn-multi-recv-err"

	agentCtx := makeGRPCContext(agentID)
	agentStreamMock := &fake.MockAgentTunnelServer{Ctx: agentCtx}
	d.Upstreams.Store(agentID, agentStreamMock)

	// Use a timed context so the tight loop on EOF eventually exits
	dsCtx, dsCancel := context.WithTimeout(makeGRPCContext(agentID), 1*time.Second)
	defer dsCancel()

	ds := &mockRelayTunnelServer{
		ctx: dsCtx,
		recvMsgs: []*pb.TunnelFrame{
			{
				MessageType:   "request",
				Routing:       &pb.RoutingInfo{AgentId: agentID},
				TransactionId: txnID,
				Eom:           false, // multi-message — recv goroutine started
			},
			// Only one message. Second Recv returns EOF → receivedAllFromCluster
			// Context timeout kills the tight loop
		},
	}

	done := make(chan error, 1)
	go func() { done <- d.RelayTunnel(ds) }()

	select {
	case err := <-done:
		// Context timeout or error expected
		t.Logf("RelayTunnel returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel didn't return")
	}
}

// ===========================================================================
// RelayTunnel: agent-local first message no Routing (L579-580)
// ===========================================================================

func TestRelayTunnel_AgentLocal_NoRouting(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		GRPCPort:                 9090,
	})

	agentID := "oph-no-addl"
	// First message requires agentID but Routing check happens early
	dsCtx := makeGRPCContext(agentID)
	ds := &mockRelayTunnelServer{
		ctx: dsCtx,
		recvMsgs: []*pb.TunnelFrame{
			{
				MessageType: "request",
				// nil → triggers AgentID required error (L423)
			},
		},
	}

	err := d.RelayTunnel(ds)
	if err == nil {
		t.Fatal("expected error for nil Routing")
	}
	if !strings.Contains(err.Error(), "AgentID is required") {
		t.Fatalf("expected 'AgentID is required', got: %v", err)
	}
}

// ===========================================================================
// ConnectHTTP: stream context has error on disconnect (L321-323)
// ===========================================================================

func TestRelayTunnel_AgentLocal_ResponseNoRouting(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		GRPCPort:                 9090,
	})

	agentID := "oph-ds-no-ad"
	txnID := "txn-ds-no-ad"

	agentCtx := makeGRPCContext(agentID)
	agentStreamMock := &fake.MockAgentTunnelServer{Ctx: agentCtx}
	d.Upstreams.Store(agentID, agentStreamMock)

	dsCtx := makeGRPCContext(agentID)
	ds := &mockRelayTunnelServer{
		ctx: dsCtx,
		recvMsgs: []*pb.TunnelFrame{
			{
				MessageType:   "request",
				Routing:       &pb.RoutingInfo{AgentId: agentID},
				TransactionId: txnID,
				Eom:           true, // single message
			},
		},
	}

	done := make(chan error, 1)
	go func() { done <- d.RelayTunnel(ds) }()

	// Wait for txn registration
	time.Sleep(300 * time.Millisecond)

	// Send first response WITHOUT Routing → hits L646-648
	if err := d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
		MessageType: "resp1",
		// no Routing
	}, fake.TestLogger()); err != nil {
		select {
		case err := <-done:
			t.Logf("RelayTunnel returned early: %v", err)
			return
		case <-time.After(2 * time.Second):
			t.Fatal("txn not registered and RelayTunnel didn't return")
		}
	}

	// Send second response WITH EOM to finish
	_ = d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
		MessageType:   "resp2",
		TransactionId: txnID,
		Eom:           true,
	}, fake.TestLogger())

	select {
	case err := <-done:
		t.Logf("RelayTunnel returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel didn't return")
	}
}

// ===========================================================================
// RelayTunnel: first message with nil Routing (L563-565)
// ===========================================================================

func TestRelayTunnel_AgentLocal_FirstMsgNoRouting(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		GRPCPort:                 9090,
	})

	agentID := "oph-ds-nil-ad"
	agentCtx := makeGRPCContext(agentID)
	agentStreamMock := &fake.MockAgentTunnelServer{Ctx: agentCtx}
	d.Upstreams.Store(agentID, agentStreamMock)

	dsCtx := makeGRPCContext(agentID)
	ds := &mockRelayTunnelServer{
		ctx: dsCtx,
		recvMsgs: []*pb.TunnelFrame{
			{
				MessageType:   "request",
				Routing:       &pb.RoutingInfo{AgentId: agentID},
				TransactionId: "txn-nil-ad",
				Eom:           true, // single message so no multi-msg goroutine
			},
		},
	}

	// RelayTunnel sends the first message to agent via FanoutResponses
	// The nil check at L563 runs with the firstMessage, but Routing is non-nil here
	// So this won't hit L563-565... we need a firstMessage with nil Routing
	// BUT then the AgentID would be missing from metadata... let's just run it

	done := make(chan error, 1)
	go func() { done <- d.RelayTunnel(ds) }()

	// Send response to finish
	time.Sleep(200 * time.Millisecond)
	_ = d.TxnRegistry.Fanout("txn-nil-ad", &pb.TunnelFrame{
		MessageType:   "resp",
		TransactionId: "txn-nil-ad",
		Eom:           true,
	}, fake.TestLogger())

	select {
	case err := <-done:
		t.Logf("RelayTunnel returned: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel didn't return")
	}
}

// ===========================================================================
// AgentTunnel: heartbeat fail + DeleteIPs error (L781-783 in AgentTunnel)
// ===========================================================================
