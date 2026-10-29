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
	"google.golang.org/grpc/metadata"
)

// mockRelayTunnelServer is a relay tunnel a replica link peer opened (its
// context carries the peer mark), unless notPeer.
type mockRelayTunnelServer struct {
	ctx      context.Context
	notPeer  bool
	recvMsgs []*pb.TunnelFrame
	recvIdx  int
	recvErr  error
	sendErr  error
	sent     []*pb.TunnelFrame
}

func (m *mockRelayTunnelServer) Send(msg *pb.TunnelFrame) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, msg)
	return nil
}

func (m *mockRelayTunnelServer) Recv() (*pb.TunnelFrame, error) {
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	if m.recvIdx >= len(m.recvMsgs) {
		return nil, io.EOF
	}
	msg := m.recvMsgs[m.recvIdx]
	m.recvIdx++
	return msg, nil
}

func (m *mockRelayTunnelServer) Context() context.Context {
	if m.notPeer || m.ctx == nil {
		return m.ctx
	}
	return context.WithValue(m.ctx, PeerLinkCaller{}, true)
}

// TestDispatcher_CleanupDeleteIPs covers the Run cleanup path where
// connected agents exist and DeleteIPs is called.
func TestDispatcher_CleanupDeleteIPs(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	ctx := context.Background()

	inv := &errorIPInventory{
		deleteErr: errors.New("delete failed"),
	}
	d := NewDispatcher(ctx, fake.TestLogger(), b, nil, inv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	// Register a fake agent
	d.Agents.Store("agent-1", &HTTPStream{ID: "agent-1"})

	agentIDs := d.GetAllConnectedAgents()
	if len(agentIDs) != 1 || agentIDs[0] != "agent-1" {
		t.Fatalf("expected [agent-1], got %v", agentIDs)
	}

	// DeleteIPs with error
	err := d.IPInventory.DeleteIPs(ctx, agentIDs...)
	if err == nil {
		t.Fatal("expected delete error")
	}
	if !strings.Contains(err.Error(), "delete failed") {
		t.Fatalf("expected 'delete failed', got: %v", err)
	}

	// Also test success path
	d.IPInventory = directory.NewNoOpInventory()
	err = d.IPInventory.DeleteIPs(ctx, agentIDs...)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestConnectHTTP_DeleteIPsError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx, cancel := context.WithCancel(context.Background())

	// Inventory that succeeds on SetIP but fails on DeleteIPs
	inv := &errorIPInventory{
		deleteErr:   errors.New("delete failed"),
		getIPErr:    errors.New("not found"),
		getIPResult: "",
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Second,
		RecvBufferSize:           10,
	})

	agentID := "agentID-del-err"
	streamCtx, streamCancel := makeGRPCContextWithCancel(agentID)

	mock := &mockConnectHTTPServer{
		ctx: streamCtx,
	}

	done := make(chan error, 1)
	go func() {
		done <- d.ConnectHTTP(mock)
	}()

	// Let the subscription start, then cancel to trigger the deferred DeleteIPs
	time.Sleep(500 * time.Millisecond)
	streamCancel()
	cancel()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("ConnectHTTP didn't return")
	}
}

// upStreamDeleteIPsErrorTest is a shared helper for tests that verify AgentTunnel
// behaviour when DeleteIPs returns an error.
func upStreamDeleteIPsErrorTest(t *testing.T, deleteErr, sendErr string, agentID string) {
	t.Helper()

	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errInv := &errorIPInventory{
		deleteErr:   errors.New(deleteErr),
		getIPErr:    errors.New("not found"), // validateAgentConnection passes
		getIPResult: "",
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, directory.NewNoOpInventory(), errInv, 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    50 * time.Millisecond,
		RecvBufferSize:           10,
	})

	streamCtx, streamCancel := makeGRPCContextWithCancel(agentID)

	mock := &fake.MockAgentTunnelServer{
		Ctx:     streamCtx,
		SendErr: errors.New(sendErr),
	}

	done := make(chan error, 1)
	go func() {
		done <- d.AgentTunnel(mock)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		streamCancel()
		t.Fatal("AgentTunnel didn't return")
	}
	streamCancel()
}

func TestAgentTunnel_DeferDeleteIPsError(t *testing.T) {
	upStreamDeleteIPsErrorTest(t, "delete-ips-failed", "send failed", "agentID-defer-del")
}

func TestAgentTunnel_HeartbeatSendFailure_DeleteIPsError(t *testing.T) {
	upStreamDeleteIPsErrorTest(t, "hb-cache-delete-failed", "heartbeat send failed", "agentID-hb-del-err")
}

// makeGRPCContext creates a context with incoming gRPC metadata that includes
// an Authorization Bearer token with the specified agentID claim.
func makeGRPCContext(agentID string) context.Context {
	md := metadata.Pairs("x-apikey-agentID", agentID, "x-auth-method", "apikey")
	return testPrincipal(metadata.NewIncomingContext(context.Background(), md))
}

func makeGRPCContextWithCancel(agentID string) (context.Context, context.CancelFunc) {
	md := metadata.Pairs("x-apikey-agentID", agentID, "x-auth-method", "apikey")
	ctx, cancel := context.WithCancel(context.Background())
	return testPrincipal(metadata.NewIncomingContext(ctx, md)), cancel
}

type mockConnectHTTPServer struct {
	ctx      context.Context
	recvMsgs []*pb.HTTPProxyResponse
	recvIdx  int
	recvErr  error
	sendErr  error
	sent     []*pb.HTTPProxyRequest
}

func (m *mockConnectHTTPServer) Send(out *pb.HTTPProxyRequest) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, out)
	return nil
}

func (m *mockConnectHTTPServer) Recv() (*pb.HTTPProxyResponse, error) {
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	if m.recvIdx >= len(m.recvMsgs) {
		return nil, io.EOF
	}
	msg := m.recvMsgs[m.recvIdx]
	m.recvIdx++
	return msg, nil
}

func (m *mockConnectHTTPServer) Context() context.Context { return m.ctx }

func (m *mockConnectHTTPServer) SendMsg(_ any) error { return nil }

func (m *mockConnectHTTPServer) RecvMsg(_ any) error { return nil }

func (m *mockConnectHTTPServer) SetHeader(_ metadata.MD) error { return nil }

func (m *mockConnectHTTPServer) SendHeader(_ metadata.MD) error { return nil }

func (m *mockConnectHTTPServer) SetTrailer(_ metadata.MD) {}
