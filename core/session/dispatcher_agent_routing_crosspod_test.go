// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
)

// ---------------------------------------------------------------------------
// Cross-pod A2A integration tests (bufconn)
// ---------------------------------------------------------------------------

// TestRelayTunnel_CrossPodA2A_TargetAgentIdRouting verifies that the RelayTunnel
// handler on the receiving pod correctly routes using Metadata.TargetAgentId
// (the intended recipient) rather than Metadata.AgentID (the source agent).
// This is the core mechanism enabling cross-pod agent-to-agent communication.
//
// Simulated topology:
//
//	pod-1: agent-a (sender)  ->  routeAgentToAgentRemote  ->  dialPeerStream
//	                                                              |
//	pod-2 (this test): agent-b (receiver)  <-  RelayTunnel handler <-|
//
// The test acts as the pod-1 side, sending a RelayTunnel message with
// AgentID="agent-a" and TargetAgentId="agent-b" to the bufconn server (pod-2).
func TestRelayTunnel_CrossPodA2A_TargetAgentIdRouting(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register agent-b as echo agent on this "pod".
	registerEchoAgent(ctx, d, "agent-b")

	// Open RelayTunnel - simulates what routeAgentToAgentRemote on pod-1 does.
	stream, err := client.RelayTunnel(ctx)
	if err != nil {
		t.Fatalf("RelayTunnel dial: %v", err)
	}

	// Send a cross-pod A2A message: source is agent-a (on pod-1),
	// target is agent-b (on this pod).
	msg := &pb.TunnelFrame{
		Payload:       []byte("cross-pod-hello"),
		MessageType:   "test",
		TransactionId: "txn-cross-pod-1",
		Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
		Routing: &pb.RoutingInfo{
			AgentId:       "agent-a", // source agent on pod-1
			TargetAgentId: "agent-b", // target agent on this pod
		},
		Eom: true,
	}
	if err := stream.Send(msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Receive response - agent-b's echo stream replies with the same payload.
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}

	if got := string(resp.GetPayload()); got != "cross-pod-hello" {
		t.Fatalf("expected echoed payload 'cross-pod-hello', got: %q", got)
	}
	if !resp.GetEom() {
		t.Fatal("expected EOM on response")
	}
}

// TestRelayTunnel_CrossPodA2A_TargetNotLocal verifies that when a cross-pod A2A
// message arrives but the target agent is not on this pod either, the
// RelayTunnel handler correctly attempts to forward to the next pod (and fails
// with our NoOp inventory, producing a proper error).
func TestRelayTunnel_CrossPodA2A_TargetNotLocal(t *testing.T) {
	client, _, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Do NOT register agent-b - it's not on this pod either.

	stream, err := client.RelayTunnel(ctx)
	if err != nil {
		t.Fatalf("RelayTunnel dial: %v", err)
	}

	msg := &pb.TunnelFrame{
		Payload:       []byte("hello"),
		MessageType:   "test",
		TransactionId: "txn-cross-pod-miss",
		Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
		Routing: &pb.RoutingInfo{
			AgentId:       "agent-a",
			TargetAgentId: "agent-b",
		},
		Eom: true,
	}
	if err := stream.Send(msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Expect an error on Recv since agent-b is not found anywhere (NoOp inventory).
	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected error when target agent not found, got nil")
	}
}

// TestRelayTunnel_NormalRouting_AgentIDWithoutTargetAgentId is a regression test
// verifying that normal RelayTunnel routing (service-to-agent, no TargetAgentId)
// continues to work correctly - the fix for cross-pod A2A must not break the
// default path.
func TestRelayTunnel_NormalRouting_AgentIDWithoutTargetAgentId(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register agent-a as echo agent (standard RelayTunnel target).
	registerEchoAgent(ctx, d, "agent-a")

	stream, err := client.RelayTunnel(ctx)
	if err != nil {
		t.Fatalf("RelayTunnel dial: %v", err)
	}

	// Standard RelayTunnel message - no TargetAgentId.
	msg := &pb.TunnelFrame{
		Payload:       []byte("svc-request"),
		MessageType:   "test",
		TransactionId: "txn-normal-1",
		Timestamps:    &pb.Timestamps{RequestAtGrpcServer: time.Now().UnixMilli()},
		Routing: &pb.RoutingInfo{
			AgentId: "agent-a", // target agent, no TargetAgentId
		},
		Eom: true,
	}
	if err := stream.Send(msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}

	if got := string(resp.GetPayload()); got != "svc-request" {
		t.Fatalf("expected echoed payload 'svc-request', got: %q", got)
	}
	if !resp.GetEom() {
		t.Fatal("expected EOM on response")
	}
}
