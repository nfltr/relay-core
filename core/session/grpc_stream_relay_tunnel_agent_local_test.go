// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
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
// ConnectHTTP tests
// ===========================================================================

func TestRelayTunnel_AgentLocal_MultiMessage(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	d.ClusterRelayPeers = true // the relay caller stands in for a peer pod
	agentID := "agentID-multi-msg"
	// Register an agent stream directly for the RelayTunnel to find
	agentMock := &fake.MockAgentTunnelServer{
		Ctx: makeGRPCContext(agentID),
	}
	agentStream := NewGRPCStream(agentID, agentMock, agentMock.Ctx, make(chan proto.Message, 10), time.NewTicker(5*time.Second))
	d.Upstreams.Store(agentID, agentStream)

	// Create RelayTunnel mock with multi-message: first message Eom=false, second Eom=true
	firstMsg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-multi-msg",
		Eom:           false,
	}
	secondMsg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-multi-msg",
		Eom:           true,
	}

	dsMock := &mockRelayTunnelServer{
		ctx:      context.Background(),
		recvMsgs: []*pb.TunnelFrame{firstMsg, secondMsg},
	}

	// Start RelayTunnel in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- d.RelayTunnel(dsMock)
	}()

	// Wait a bit for messages to be sent to the agent stream then send response
	time.Sleep(200 * time.Millisecond)

	// The RelayTunnel registered a transaction and is waiting for a response
	// Send a response via the transaction registry
	if err := d.TxnRegistry.Fanout("txn-multi-msg", &pb.TunnelFrame{
		TransactionId: "txn-multi-msg",
		Eom:           true,
	}, fake.TestLogger()); err != nil {
		t.Fatal("expected transaction to be registered")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("RelayTunnel returned: %v (may be expected)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel did not complete")
	}
}

// ===========================================================================
// RelayTunnel: response channel close path
// ===========================================================================

func TestRelayTunnel_AgentLocal_ResponseChannelClosed(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-resp-close"
	agentMock := &fake.MockAgentTunnelServer{Ctx: makeGRPCContext(agentID)}
	agentStream := NewGRPCStream(agentID, agentMock, agentMock.Ctx, make(chan proto.Message, 10), time.NewTicker(5*time.Second))
	d.Upstreams.Store(agentID, agentStream)

	msg := &pb.TunnelFrame{
		MessageType:   "test",
		Routing:       &pb.RoutingInfo{AgentId: agentID},
		TransactionId: "txn-resp-close",
		Eom:           true,
	}

	dsMockCtx, dsMockCancel := context.WithCancel(context.Background())
	defer dsMockCancel()
	dsMock := &mockRelayTunnelServer{
		ctx:      dsMockCtx,
		recvMsgs: []*pb.TunnelFrame{msg},
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- d.RelayTunnel(dsMock)
	}()

	time.Sleep(200 * time.Millisecond)

	// Cancel the stream context to signal RelayTunnel to exit.
	// cleanup (deferred inside RelayTunnel) closes the channel after the function returns.
	dsMockCancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Logf("RelayTunnel returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RelayTunnel did not complete")
	}
}
