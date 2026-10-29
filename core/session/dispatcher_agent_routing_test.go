// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const dispatcherRoutingRequestPayload = "request"

// ---------------------------------------------------------------------------
// isAgentToAgentRequest
// ---------------------------------------------------------------------------

func TestIsAgentToAgentRequest_True(t *testing.T) {
	msg := &pb.TunnelFrame{
		TransactionId: "txn-1",
		Routing: &pb.RoutingInfo{
			AgentId:       "agent-a",
			TargetAgentId: "agent-b",
		},
	}
	if !isAgentToAgentRequest(msg) {
		t.Fatal("expected true")
	}
}

func TestIsAgentToAgentRequest_False_NoTargetAgentId(t *testing.T) {
	msg := &pb.TunnelFrame{
		TransactionId: "txn-1",
		Routing:       &pb.RoutingInfo{AgentId: "agent-a"},
	}
	if isAgentToAgentRequest(msg) {
		t.Fatal("expected false when target_agent_id is empty")
	}
}

func TestIsAgentToAgentRequest_False_NilRouting(t *testing.T) {
	msg := &pb.TunnelFrame{TransactionId: "txn-1"}
	if isAgentToAgentRequest(msg) {
		t.Fatal("expected false when metadata is nil")
	}
}

func TestIsAgentToAgentRequest_False_EmptyFrame(t *testing.T) {
	if isAgentToAgentRequest(&pb.TunnelFrame{}) {
		t.Fatal("expected false when additional_data is nil")
	}
}

// ---------------------------------------------------------------------------
// isA2ACancelMessage
// ---------------------------------------------------------------------------

func TestIsA2ACancelMessage(t *testing.T) {
	cancel := &pb.TunnelFrame{MessageType: msgTypeCancel}
	if !isA2ACancelMessage(cancel) {
		t.Fatal("expected true for cancel message")
	}
	normal := &pb.TunnelFrame{MessageType: "test"}
	if isA2ACancelMessage(normal) {
		t.Fatal("expected false for non-cancel message")
	}
}

// ---------------------------------------------------------------------------
// a2aContextFromMsg — deadline propagation
// ---------------------------------------------------------------------------

func TestA2AContextFromMsg_WithDeadline(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	msg := &pb.TunnelFrame{
		Routing: &pb.RoutingInfo{DeadlineMs: deadline.UnixMilli()},
	}
	ctx, cancel := a2aContextFromMsg(context.Background(), msg)
	defer cancel()

	dl, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected deadline on context")
	}
	// Allow 100ms tolerance.
	if dl.Sub(deadline).Abs() > 100*time.Millisecond {
		t.Fatalf("deadline mismatch: expected ~%v, got %v", deadline, dl)
	}
}

func TestA2AContextFromMsg_WithoutDeadline(t *testing.T) {
	msg := &pb.TunnelFrame{
		Routing: &pb.RoutingInfo{AgentId: "a", TargetAgentId: "b"},
	}
	ctx, cancel := a2aContextFromMsg(context.Background(), msg)
	defer cancel()

	if _, ok := ctx.Deadline(); ok {
		t.Fatal("expected no deadline")
	}
}

// ---------------------------------------------------------------------------
// Feature 1: Deadline propagation — transaction times out
// ---------------------------------------------------------------------------

func TestDispatcher_routeAgentToAgentLocal_DeadlineExceeded(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	// Target agent that never responds — deadline will fire.
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	// Deadline 200ms from now.
	deadlineMs := time.Now().Add(200 * time.Millisecond).UnixMilli()

	session := &a2aSession{
		requestCh:  make(chan *pb.TunnelFrame, 1),
		responseCh: make(chan *pb.TunnelFrame, 10),
		cancelFn:   func() {},
	}
	msg := &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-deadline-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b", DeadlineMs: deadlineMs},
		Eom:           true,
	}
	session.requestCh <- msg
	close(session.requestCh)

	done := make(chan struct{})
	go func() {
		d.routeAgentToAgent(ctx, sourceStream, session, "txn-deadline-1")
		close(done)
	}()

	// Should complete within ~300ms (200ms deadline + slack).
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("routeAgentToAgent did not return after deadline")
	}

	// Source should receive a DEADLINE_EXCEEDED error.
	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetStatusCode() != int32(4) { // codes.DeadlineExceeded = 4
			t.Fatalf("expected status_code=4 (DeadlineExceeded), got %d", errMsg.GetStatusCode())
		}
		if errMsg.GetStatusDetail() == "" {
			t.Fatal("expected non-empty status_detail")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for deadline error")
	}

	// Target should receive a cancel message.
	select {
	case cancelMsg := <-targetStreamer.sendCh:
		// First message is the original request; skip it.
		if cancelMsg.GetMessageType() != msgTypeCancel {
			// Read the next one.
			select {
			case cancelMsg = <-targetStreamer.sendCh:
				if cancelMsg.GetMessageType() != msgTypeCancel {
					t.Fatalf("expected cancel message, got type=%s", cancelMsg.GetMessageType())
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timeout waiting for cancel message to target")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for messages to target")
	}
}

// ---------------------------------------------------------------------------
// Feature 2: Cancellation propagation
// ---------------------------------------------------------------------------

func TestDispatcher_dispatchAgentToAgent_CancelMessage(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	txnID := "txn-cancel-dispatch"

	// Start a transaction (non-EOM first message).
	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("req1"),
		MessageType:   "test",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           false,
	})

	// Give the session goroutine time to start.
	time.Sleep(50 * time.Millisecond)

	// Verify session exists.
	if _, ok := d.a2aSessions.Load(txnID); !ok {
		t.Fatal("expected session to exist")
	}

	// Send cancel message.
	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		MessageType:   msgTypeCancel,
		TransactionId: txnID,
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
	})

	// Session should be cleaned up after cancellation.
	time.Sleep(200 * time.Millisecond)
	if _, ok := d.a2aSessions.Load(txnID); ok {
		t.Fatal("expected session to be cleaned up after cancel")
	}
}

func TestDispatcher_dispatchAgentToAgent_CapsSessionBufferSize(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	d.Config.RecvBufferSize = 1000
	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	// The session as dispatchAgentToAgent makes it: its route ends at once
	// (agent-b is offline) and drops it from a2aSessions, so the test does
	// not look it up there.
	session, _ := d.newA2ASession(ctx, sourceStream)
	session.cancelFn()

	if got := cap(session.requestCh); got != a2aSessionBufferCap {
		t.Fatalf("requestCh cap = %d, want %d", got, a2aSessionBufferCap)
	}
	if got := cap(session.responseCh); got != a2aSessionBufferCap {
		t.Fatalf("responseCh cap = %d, want %d", got, a2aSessionBufferCap)
	}
}

func TestDispatcher_routeAgentToAgentLocal_ContextCancelled_PropagatesCancel(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx, ctxCancel := context.WithCancel(context.Background())

	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	// The target agent's stream has its own context: only the source cancels.
	// Sharing ctx made the cancel also a target disconnect, which the route
	// reports to the source instead of cancelling a gone target
	// (bindToAgentStream; TestLocalRouteReportsTargetAgentDisconnectToSource),
	// so the test failed whenever that branch won the race.
	targetCtx := context.Background()
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: targetCtx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, targetCtx)
	d.Upstreams.Store("agent-b", targetStream)

	session := &a2aSession{
		requestCh:  make(chan *pb.TunnelFrame, 1),
		responseCh: make(chan *pb.TunnelFrame, 10),
		cancelFn:   func() {},
	}
	msg := &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-ctx-cancel-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           true,
	}
	session.requestCh <- msg
	close(session.requestCh)

	// Cancel context after first message reaches target.
	go func() {
		<-targetStreamer.sendCh
		time.Sleep(50 * time.Millisecond)
		ctxCancel()
	}()

	done := make(chan struct{})
	go func() {
		d.routeAgentToAgent(ctx, sourceStream, session, "txn-ctx-cancel-1")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not return after context cancellation")
	}

	// Target should receive a cancel message after the original request.
	var gotCancel bool
	timeout := time.After(2 * time.Second)
	for !gotCancel {
		select {
		case m := <-targetStreamer.sendCh:
			if m.GetMessageType() == msgTypeCancel {
				gotCancel = true
			}
		case <-timeout:
			t.Fatal("timeout waiting for cancel message to target")
		}
	}
}

// ---------------------------------------------------------------------------
// Feature 3: Structured status codes
// ---------------------------------------------------------------------------

func TestDispatcher_sendAgentToAgentError_HasStatusFields(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 1), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	originalMsg := &pb.TunnelFrame{
		TransactionId: "txn-status-1",
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
	}

	d.sendAgentToAgentError(sourceStream, originalMsg, 14, "target unavailable", fake.TestLogger())

	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError {
			t.Fatalf("expected 'error' message type, got: %s", errMsg.GetMessageType())
		}
		if errMsg.GetStatusCode() != 14 {
			t.Fatalf("expected status_code=14, got %d", errMsg.GetStatusCode())
		}
		if errMsg.GetStatusDetail() != "target unavailable" {
			t.Fatalf("expected status_detail='target unavailable', got %q", errMsg.GetStatusDetail())
		}
		if !errMsg.GetEom() {
			t.Fatal("expected EOM on error message")
		}
		if errMsg.GetRouting().GetAgentId() != "agent-b" {
			t.Fatalf("expected agentID=agent-b, got: %s", errMsg.GetRouting().GetAgentId())
		}
		if errMsg.GetRouting().GetTargetAgentId() != "agent-a" {
			t.Fatalf("expected target_agent_id=agent-a, got: %s", errMsg.GetRouting().GetTargetAgentId())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error message")
	}
}

func TestDispatcher_sendAgentToAgentError_SendFails(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &mockStreamer{sendErr: errors.New("broken pipe")}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	originalMsg := &pb.TunnelFrame{
		TransactionId: "txn-err-fail",
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
	}
	// Should not panic.
	d.sendAgentToAgentError(sourceStream, originalMsg, 14, "target unavailable", fake.TestLogger())
}

// ---------------------------------------------------------------------------
// Feature 4: Initial metadata (response headers)
// ---------------------------------------------------------------------------

func TestDispatcher_routeAgentToAgentLocal_InitialMetadataRelayed(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	txnID := "txn-initmd-1"

	// Simulate target agent sending initial_metadata in its first response.
	go func() {
		<-targetStreamer.sendCh // drain the request
		time.Sleep(20 * time.Millisecond)

		// Response with initial_metadata.
		_ = d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
			Payload:       []byte("resp"),
			MessageType:   "test",
			TransactionId: txnID,
			Timestamps:    &pb.Timestamps{},
			Routing: &pb.RoutingInfo{
				AgentId:         "agent-b",
				InitialMetadata: map[string]string{"content-type": "application/json", "x-custom": "value1"},
			},
			Eom: true,
		}, fake.TestLogger())
	}()

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("req"),
		MessageType:   "test",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           true,
	})

	select {
	case resp := <-sourceStreamer.sendCh:
		md := resp.GetRouting().GetInitialMetadata()
		if md == nil {
			t.Fatal("expected initial_metadata in response")
		}
		if md["content-type"] != "application/json" {
			t.Fatalf("expected content-type=application/json, got %s", md["content-type"])
		}
		if md["x-custom"] != "value1" {
			t.Fatalf("expected x-custom=value1, got %s", md["x-custom"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for response with initial_metadata")
	}
}

// ---------------------------------------------------------------------------
// Feature 5: Backpressure isolation (per-session response channel)
// ---------------------------------------------------------------------------

func TestDispatcher_routeAgentToAgentLocal_BackpressureIsolation(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	// Source agent with a slow consumer — small channel, reads slowly.
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 1), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	txnID := "txn-bp-1"

	// Simulate target sending 5 quick responses; the per-session responseCh
	// absorbs them so Fanout doesn't block. Source reads at its own pace.
	go func() {
		<-targetStreamer.sendCh // drain request
		time.Sleep(20 * time.Millisecond)

		for i := range 5 {
			_ = d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
				Payload:       []byte("resp"),
				MessageType:   "test",
				TransactionId: txnID,
				Timestamps:    &pb.Timestamps{},
				Routing:       &pb.RoutingInfo{AgentId: "agent-b"},
				Eom:           i == 4,
			}, fake.TestLogger())
		}
	}()

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("req"),
		MessageType:   "test",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           true,
	})

	// Slowly read all 5 responses.
	var received int
	timeout := time.After(5 * time.Second)
	for received < 5 {
		select {
		case <-sourceStreamer.sendCh:
			received++
			time.Sleep(20 * time.Millisecond) // slow consumer
		case <-timeout:
			t.Fatalf("timeout: received %d/5 responses", received)
		}
	}
}

// ---------------------------------------------------------------------------
// N:N streaming (preserved from before)
// ---------------------------------------------------------------------------

func TestDispatcher_processGRPCInput_AgentToAgent_SingleMessage(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	input := &pb.TunnelFrame{
		Payload:       []byte("hello"),
		MessageType:   "test",
		TransactionId: "txn-single-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           true,
	}

	go func() {
		<-targetStreamer.sendCh
		time.Sleep(20 * time.Millisecond)
		_ = d.TxnRegistry.Fanout("txn-single-1", &pb.TunnelFrame{
			Payload:       []byte("hello"),
			MessageType:   "test",
			TransactionId: "txn-single-1",
			Timestamps:    &pb.Timestamps{},
			Routing:       &pb.RoutingInfo{AgentId: "agent-b"},
			Eom:           true,
		}, fake.TestLogger())
	}()

	err := d.processGRPCInput(ctx, sourceStream, input, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case resp := <-sourceStreamer.sendCh:
		if string(resp.GetPayload()) != "hello" {
			t.Fatalf("unexpected response: %s", string(resp.GetPayload()))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for response")
	}
}

func TestDispatcher_routeAgentToAgentLocal_NtoN(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	txnID := "txn-nn-1"

	go func() {
		var received int
		for msg := range targetStreamer.sendCh {
			received++
			if msg.GetEom() {
				break
			}
		}
		for i := range 2 {
			time.Sleep(10 * time.Millisecond)
			_ = d.TxnRegistry.Fanout(txnID, &pb.TunnelFrame{
				Payload:       []byte("resp"),
				MessageType:   "test",
				TransactionId: txnID,
				Timestamps:    &pb.Timestamps{},
				Routing:       &pb.RoutingInfo{AgentId: "agent-b"},
				Eom:           i == 1,
			}, fake.TestLogger())
		}
	}()

	for i := range 3 {
		d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
			Payload:       []byte("req"),
			MessageType:   "test",
			TransactionId: txnID,
			Timestamps:    &pb.Timestamps{},
			Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
			Eom:           i == 2,
		})
	}

	var responses int
	timeout := time.After(5 * time.Second)
	for responses < 2 {
		select {
		case resp := <-sourceStreamer.sendCh:
			responses++
			if responses == 2 && !resp.GetEom() {
				t.Fatal("expected EOM on last response")
			}
		case <-timeout:
			t.Fatalf("timeout: received %d/2 responses", responses)
		}
	}
}

func TestDispatcher_routeAgentToAgentLocal_TargetSendError(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	targetStreamer := &mockStreamer{sendErr: errors.New("send failed")}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)
	d.Upstreams.Store("agent-b", targetStream)

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-err-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           true,
	})

	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError {
			t.Fatalf("expected error message type, got: %s", errMsg.GetMessageType())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for error response")
	}
}

func TestDispatcher_routeAgentToAgent_TargetNotConnected(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-nf-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
		Eom:           true,
	})

	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError {
			t.Fatalf("expected error message type, got: %s", errMsg.GetMessageType())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for error response")
	}
}

// podToPodConnectionFailuresValue reads the current total of
// rpc_server_dispatcher_pod_to_pod_connection_failures_total (every
// target_pod) straight from the default Prometheus registry, where the
// metrics package registers.
func podToPodConnectionFailuresValue(t *testing.T) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	total := 0.0
	for _, mf := range families {
		if mf.GetName() != "rpc_server_dispatcher_pod_to_pod_connection_failures_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}

// TestDispatcher_routeAgentToAgent_GenuineDialFailureRecordsPodToPodMetric
// pins a regression found by a fault-injection run (a replica killed
// mid-dial):
// dispatcher_pod_to_pod_connection_failures_total was only ever registered
// when an already-established peer stream broke mid-flight
// (cleanupPeerStream / the response-relay goroutine); a fresh cross-pod dial
// that fails to establish a connection at all — the exact failure a real
// "downstream pod died" chaos scenario produces — recorded
// dispatcher_a2a_errors_total instead and never touched the pod-to-pod
// metric, even though "a cross-pod relay could never connect" is squarely
// what that metric exists to surface. Distinct from
// TestDispatcher_routeAgentToAgent_TargetNotConnected: that test's
// codes.NotFound path (no pod dialer configured / not in multi-pod mode) is
// deliberately excluded, since it's the expected outcome in a
// single-pod/standalone deployment, not a connection failure.
func TestDispatcher_routeAgentToAgent_GenuineDialFailureRecordsPodToPodMetric(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	const targetAgentID = "agent-podfail-target"
	d.PodDialer = &failingPodDialer{err: status.Error(codes.Unavailable, "reached max retries for pod to pod connection: 10.0.0.5")}
	before := podToPodConnectionFailuresValue(t)

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-podfail-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: targetAgentID},
		Eom:           true,
	})

	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError {
			t.Fatalf("expected error message type, got: %s", errMsg.GetMessageType())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for error response")
	}

	if after := podToPodConnectionFailuresValue(t); after != before+1 {
		t.Fatalf("dispatcher_pod_to_pod_connection_failures_total = %v, want %v — a genuine cross-pod dial failure must be counted, not just target_not_found errors", after, before+1)
	}
}

func TestDispatcher_routeAgentToAgent_ExpiredDeadlineSkipsRemoteDial(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	calledDial := make(chan struct{}, 1)
	d.PodDialer = &forbiddenPodDialer{called: calledDial}

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-expired-deadline-1",
		Timestamps:    &pb.Timestamps{},
		Routing: &pb.RoutingInfo{
			AgentId:       "agent-a",
			TargetAgentId: "agent-b",
			DeadlineMs:    time.Now().Add(-100 * time.Millisecond).UnixMilli(),
		},
		Eom: true,
	})

	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError {
			t.Fatalf("expected error message type, got: %s", errMsg.GetMessageType())
		}
		if errMsg.GetStatusCode() != int32(codes.DeadlineExceeded) {
			t.Fatalf("status_code=%d, want DeadlineExceeded", errMsg.GetStatusCode())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for expired-deadline error response")
	}

	select {
	case <-calledDial:
		t.Fatal("expired transaction called remote pod dialer")
	default:
	}
}

func TestDispatcher_routeAgentToAgent_RejectsSelfRoute(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-self", sourceStreamer, ctx)
	d.Upstreams.Store("agent-self", sourceStream)

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("data"),
		MessageType:   "test",
		TransactionId: "txn-self-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "agent-self", TargetAgentId: "agent-self"},
		Eom:           true,
	})

	select {
	case errMsg := <-sourceStreamer.sendCh:
		if errMsg.GetMessageType() != msgTypeError {
			t.Fatalf("expected immediate error message type, got: %s", errMsg.GetMessageType())
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for self-route error response")
	}
}

// ---------------------------------------------------------------------------
// sendAgentToAgentCancel
// ---------------------------------------------------------------------------

func TestDispatcher_sendAgentToAgentCancel(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 1), ctx: ctx}
	targetStream := newTestGRPCStream("agent-b", targetStreamer, ctx)

	originalMsg := &pb.TunnelFrame{
		TransactionId: "txn-cancel-msg",
		Routing:       &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-b"},
	}

	d.sendAgentToAgentCancel(targetStream, originalMsg, "txn-cancel-msg", fake.TestLogger())

	select {
	case msg := <-targetStreamer.sendCh:
		if msg.GetMessageType() != "cancel" {
			t.Fatalf("expected 'cancel' message type, got: %s", msg.GetMessageType())
		}
		if msg.GetStatusCode() != int32(1) { // codes.Canceled = 1
			t.Fatalf("expected status_code=1, got %d", msg.GetStatusCode())
		}
		if !msg.GetEom() {
			t.Fatal("expected EOM on cancel message")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cancel message")
	}
}

// A2A source identity must be the relay-authenticated stream identity, never
// the client-claimed Routing.AgentId. A key-scoped worker's rpcclient stamps
// its raw --name; if forwarded unchanged the target (e.g. the relay-owned
// orchestrator handling task.resume) keys recovery on an id the dispatcher
// never registered and every reattach fails with "target agent not connected".
func TestDispatcher_dispatchAgentToAgent_StampsAuthenticatedSourceAgentID(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()
	const scoped = "nfltr-local-loopback.host-a.X-claude-code-1"
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream(scoped, sourceStreamer, ctx)

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	d.Upstreams.Store("nfltr-relay-orchestrator", newTestGRPCStream("nfltr-relay-orchestrator", targetStreamer, ctx))

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("task.resume"),
		TransactionId: "txn-stamp-source",
		Routing:       &pb.RoutingInfo{AgentId: "host-a.X-claude-code-1", TargetAgentId: "nfltr-relay-orchestrator"},
		Eom:           true,
	})

	select {
	case got := <-targetStreamer.sendCh:
		if id := got.GetRouting().GetAgentId(); id != scoped {
			t.Fatalf("target saw source agent id %q, want authenticated %q", id, scoped)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for forwarded frame")
	}
}
