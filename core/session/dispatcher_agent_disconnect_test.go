// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The caller learns why: a typed Unavailable naming the disconnected agent,
// not a generic routing failure.
func TestRelayTunnelReportsTargetAgentDisconnectAsUnavailable(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agentCtx, disconnectAgent := context.WithCancel(ctx)
	registerEchoAgent(agentCtx, d, "worker-c")
	stream, err := client.RelayTunnel(ctx)
	if err != nil {
		t.Fatalf("RelayTunnel dial: %v", err)
	}
	if err := stream.Send(&pb.TunnelFrame{
		Payload: []byte("task.assign"), MessageType: "a2a-request", TransactionId: "txn-task-stream-2",
		Routing: &pb.RoutingInfo{AgentId: "orchestrator", TargetAgentId: "worker-c"},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first reply: %v", err)
	}
	disconnectAgent()
	_, err = stream.Recv()
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), errTargetAgentDisconnected.Error()) {
		t.Fatalf("Recv error = %v, want Unavailable %q", err, errTargetAgentDisconnected)
	}
}

// kind Tier B F7 (fake-claude round): the worker was connected to the relay
// that owned its task, so the task stream took the local route. Its disconnect
// ended the transaction with a context.Canceled error, which the local route
// read as "the source cancelled": it tried to cancel the gone worker and sent
// the source nothing. The owner never marked the task disconnected, dropped the
// reconnected worker's task.resume, and the phase sat `running` holding the
// worker's slot. Invariant (same as the RelayTunnel route): the source of a
// transaction whose target agent disconnected receives a typed Unavailable.
func TestLocalRouteReportsTargetAgentDisconnectToSource(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("orchestrator", sourceStreamer, ctx)
	targetCtx, disconnectTarget := context.WithCancel(ctx)
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: targetCtx}
	d.Upstreams.Store("worker-l", newTestGRPCStream("worker-l", targetStreamer, targetCtx))

	session := &a2aSession{requestCh: make(chan *pb.TunnelFrame, 1), responseCh: make(chan *pb.TunnelFrame, 10), cancelFn: func() {}}
	session.requestCh <- &pb.TunnelFrame{
		Payload: []byte("task.assign"), MessageType: "a2a-request", TransactionId: "txn-local-disconnect",
		Timestamps: &pb.Timestamps{}, Routing: &pb.RoutingInfo{AgentId: "orchestrator", TargetAgentId: "worker-l"},
	}
	done := make(chan struct{})
	go func() {
		d.routeAgentToAgent(ctx, sourceStream, session, "txn-local-disconnect")
		close(done)
	}()
	<-targetStreamer.sendCh // the task reached the worker
	disconnectTarget()      // the worker's own stream ends
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("local route to a disconnected agent stayed open")
	}
	select {
	case msg := <-sourceStreamer.sendCh:
		if codes.Code(msg.GetStatusCode()) != codes.Unavailable || msg.GetStatusDetail() != errTargetAgentDisconnected.Error() {
			t.Fatalf("source got status %d %q, want Unavailable %q", msg.GetStatusCode(), msg.GetStatusDetail(), errTargetAgentDisconnected)
		}
	default:
		t.Fatal("source received nothing: the caller never learns the worker is gone")
	}
}

// F6: when one worker's peer stream ended because that worker disconnected,
// the caller evicted the pooled connection to the peer relay, resetting every
// other worker's task stream on it ("remote routing error"). A target-agent
// disconnect is not a pod-to-pod failure.
func TestCleanupPeerStreamKeepsConnectionOnTargetAgentDisconnect(t *testing.T) {
	d := &Dispatcher{}
	evicted := false
	result := &PodRelayStream{cleanup: func() { evicted = true }}
	peerErr := status.Error(codes.Unavailable, errTargetAgentDisconnected.Error())
	d.cleanupPeerStream(result, "worker-c", fmt.Errorf("receive message from next pod failed: %v: %w", peerErr, podToPodStreamErr), fake.TestLogger())
	if evicted {
		t.Fatal("a peer-reported target-agent disconnect evicted the shared pod-to-pod connection")
	}
	d.cleanupPeerStream(result, "worker-c", fmt.Errorf("receive message from next pod failed: %v: %w", status.Error(codes.Unavailable, "connection refused"), podToPodStreamErr), fake.TestLogger())
	if !evicted {
		t.Fatal("a genuine pod-to-pod failure must still evict the connection")
	}
}

// kind Tier B round 2 (F5): a worker pod was killed mid-task while its relay
// stayed up. The owner relay's cross-pod task stream to it never ended, so the
// task sat "running" with no transport_disconnected and the phase held the
// worker's slot forever. Invariant: an A2A transaction bound to an agent ends,
// with an error, when that agent's stream ends.
func TestRelayTunnelEndsWhenTargetAgentDisconnects(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	agentCtx, disconnectAgent := context.WithCancel(ctx)
	registerEchoAgent(agentCtx, d, "worker-b")

	stream, err := client.RelayTunnel(ctx)
	if err != nil {
		t.Fatalf("RelayTunnel dial: %v", err)
	}
	// A long-lived task stream: the first frame does not end the request.
	if err := stream.Send(&pb.TunnelFrame{
		Payload: []byte("task.assign"), MessageType: "a2a-request", TransactionId: "txn-task-stream",
		Routing: &pb.RoutingInfo{AgentId: "orchestrator", TargetAgentId: "worker-b"},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("first reply: %v", err)
	}

	disconnectAgent() // the worker's own stream ends (pod killed)

	done := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Recv after target disconnect returned a frame, want the transaction to end with an error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RelayTunnel to a disconnected agent stayed open: the caller never learns the worker is gone")
	}
}
