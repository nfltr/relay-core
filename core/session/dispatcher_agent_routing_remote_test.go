// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nfltr/relay/client/p2p"
	"github.com/nfltr/relay/client/rpcclient"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
)

func TestDispatcher_routeAgentToAgentRemote_ResponseBeforeRequestEOM(t *testing.T) {
	remoteClient, remoteDispatcher, remoteCleanup := newA2ABufConnEnv(t)
	defer remoteCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	registerEchoAgent(ctx, remoteDispatcher, "agent-b")

	sourceDispatcher, sourceCancel := newTestDispatcher(t)
	defer sourceCancel()
	defer viper.Reset()
	sourceDispatcher.PodDialer = &relayOnlyPodDialer{client: remoteClient, ctx: ctx}

	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("agent-a", sourceStreamer, ctx)

	txnID := "txn-cross-pod-interleaved"
	firstMsg := &pb.TunnelFrame{
		Payload:       []byte("offer"),
		MessageType:   "p2p.v1",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing: &pb.RoutingInfo{
			AgentId:       "agent-a",
			TargetAgentId: "agent-b",
		},
		Eom: false,
	}

	sourceDispatcher.dispatchAgentToAgent(ctx, sourceStream, firstMsg)

	select {
	case resp := <-sourceStreamer.sendCh:
		if got := string(resp.GetPayload()); got != "offer" {
			t.Fatalf("expected first response payload 'offer', got %q", got)
		}
		if resp.GetEom() {
			t.Fatal("expected first response before request EOM")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for response before request EOM")
	}

	sourceDispatcher.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("relay"),
		MessageType:   "p2p.v1",
		TransactionId: txnID,
		Timestamps:    &pb.Timestamps{},
		Routing: &pb.RoutingInfo{
			AgentId:       "agent-a",
			TargetAgentId: "agent-b",
		},
		Eom: true,
	})

	select {
	case resp := <-sourceStreamer.sendCh:
		if got := string(resp.GetPayload()); got != "relay" {
			t.Fatalf("expected final response payload 'relay', got %q", got)
		}
		if !resp.GetEom() {
			t.Fatal("expected final response EOM after request CloseSend")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for final response")
	}
}

// ---------------------------------------------------------------------------
// A2A scoped target resolution
// ---------------------------------------------------------------------------

func TestDispatcher_routeAgentToAgent_ScopedTargetResolution(t *testing.T) { //nolint:dupl // distinct scenario
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	// Register the target agent with the scoped name "mykey.listener".
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("mykey.listener", targetStreamer, ctx)
	d.Upstreams.Store("mykey.listener", targetStream)

	// Register the source agent with the scoped name "mykey.sender".
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("mykey.sender", sourceStreamer, ctx)
	d.Upstreams.Store("mykey.sender", sourceStream)

	// Store the key base ID so the resolver knows the source's key prefix.
	d.AgentKeyBase.Store("mykey.sender", "mykey")

	// Sender targets "listener" (unscoped). The router should resolve to
	// "mykey.listener" via key scoping.
	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("hello"),
		MessageType:   "a2a-request",
		TransactionId: "txn-scope-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "mykey.sender", TargetAgentId: "listener"},
		Eom:           true,
	})

	// The target should receive the message (routed locally).
	select {
	case msg := <-targetStreamer.sendCh:
		if string(msg.GetPayload()) != "hello" {
			t.Fatalf("expected payload 'hello', got %q", string(msg.GetPayload()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: scoped target resolution failed — target did not receive message")
	}
}

func TestDispatcher_routeAgentToAgent_ExactMatchPreferred(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	// Register an agent with the exact unscoped name "listener".
	exactStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	exactStream := newTestGRPCStream("listener", exactStreamer, ctx)
	d.Upstreams.Store("listener", exactStream)

	// Also register a scoped variant "mykey.listener" — this should NOT be used.
	scopedStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	scopedStream := newTestGRPCStream("mykey.listener", scopedStreamer, ctx)
	d.Upstreams.Store("mykey.listener", scopedStream)

	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("mykey.sender", sourceStreamer, ctx)
	d.Upstreams.Store("mykey.sender", sourceStream)
	d.AgentKeyBase.Store("mykey.sender", "mykey")

	// Target "listener" — exact match should be preferred over scoping.
	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("exact"),
		MessageType:   "a2a-request",
		TransactionId: "txn-exact-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "mykey.sender", TargetAgentId: "listener"},
		Eom:           true,
	})

	select {
	case msg := <-exactStreamer.sendCh:
		if string(msg.GetPayload()) != "exact" {
			t.Fatalf("expected payload 'exact', got %q", string(msg.GetPayload()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: exact match should have been used")
	}
}

func TestDispatcher_routeAgentToAgent_NoKeyBase_UsesUniqueLocalSuffix(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	// Only scoped agent exists, and source has no key base (mTLS agent).
	// A unique local suffix is still routable so display IDs such as
	// "listener" can reach "mykey.listener" without inventing a key prefix.
	scopedStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	scopedStream := newTestGRPCStream("mykey.listener", scopedStreamer, ctx)
	d.Upstreams.Store("mykey.listener", scopedStream)

	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("mtls-agent", sourceStreamer, ctx)
	d.Upstreams.Store("mtls-agent", sourceStream)
	// No agentKeyBase entry for mtls-agent.

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("noscope"),
		MessageType:   "a2a-request",
		TransactionId: "txn-noscope-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "mtls-agent", TargetAgentId: "listener"},
		Eom:           true,
	})

	select {
	case msg := <-scopedStreamer.sendCh:
		if string(msg.GetPayload()) != "noscope" {
			t.Fatalf("expected payload 'noscope', got %q", string(msg.GetPayload()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for unique suffix route")
	}
}

// TestDispatcher_routeAgentToAgent_CrossKey_NoDoubleScoping verifies that
// when a scoped agent (key-A.sender) targets a fully-qualified agent from a
// DIFFERENT key namespace (key-B.listener), the router does NOT double-prefix
// the target (which would produce key-A.key-B.listener — incorrect).
func TestDispatcher_routeAgentToAgent_CrossKey_NoDoubleScoping(t *testing.T) { //nolint:dupl // distinct scenario
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	// Register the target with a fully-qualified name from a different key.
	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("key-B.listener", targetStreamer, ctx)
	d.Upstreams.Store("key-B.listener", targetStream)

	// Source agent from a different key namespace.
	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("key-A.sender", sourceStreamer, ctx)
	d.Upstreams.Store("key-A.sender", sourceStream)
	d.AgentKeyBase.Store("key-A.sender", "key-A")

	// Target is the fully-qualified "key-B.listener". The router must NOT
	// prepend "key-A." to get "key-A.key-B.listener".
	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("cross-key"),
		MessageType:   "a2a-request",
		TransactionId: "txn-crosskey-1",
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "key-A.sender", TargetAgentId: "key-B.listener"},
		Eom:           true,
	})

	select {
	case msg := <-targetStreamer.sendCh:
		if string(msg.GetPayload()) != "cross-key" {
			t.Fatalf("expected payload 'cross-key', got %q", string(msg.GetPayload()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout: cross-key A2A should route to key-B.listener without double-scoping")
	}
}

func TestDispatcher_routeAgentToAgent_CrossKeySameOwner_ShortTargetResolves(t *testing.T) {
	assertCrossKeyShortTargetResolvesLocally(t, "same-user@example.com", "txn-cross-owner-1", "timeout: same-owner cross-key short target should resolve to connected scoped agent")
}

func TestDispatcher_routeAgentToAgent_CrossKeyNoOwner_ShortTargetResolvesLocally(t *testing.T) {
	assertCrossKeyShortTargetResolvesLocally(t, "", "txn-cross-no-owner-1", "timeout: no-owner short target should resolve to unique connected suffix match")
}

func assertCrossKeyShortTargetResolvesLocally(t *testing.T, owner, transactionID, timeoutMessage string) {
	t.Helper()

	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	ctx := context.Background()

	targetStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	targetStream := newTestGRPCStream("key-B.bob-laptop", targetStreamer, ctx)
	d.Upstreams.Store("key-B.bob-laptop", targetStream)
	if owner != "" {
		d.AgentOwners.Store("key-B.bob-laptop", owner)
	}

	sourceStreamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 10), ctx: ctx}
	sourceStream := newTestGRPCStream("key-A.alice-laptop", sourceStreamer, ctx)
	d.Upstreams.Store("key-A.alice-laptop", sourceStream)
	d.AgentKeyBase.Store("key-A.alice-laptop", "key-A")
	if owner != "" {
		d.AgentOwners.Store("key-A.alice-laptop", owner)
	}

	d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
		Payload:       []byte("hello-bob"),
		MessageType:   "a2a-request",
		TransactionId: transactionID,
		Timestamps:    &pb.Timestamps{},
		Routing:       &pb.RoutingInfo{AgentId: "key-A.alice-laptop", TargetAgentId: "bob-laptop"},
		Eom:           true,
	})

	select {
	case msg := <-targetStreamer.sendCh:
		if string(msg.GetPayload()) != "hello-bob" {
			t.Fatalf("expected payload 'hello-bob', got %q", string(msg.GetPayload()))
		}
	case <-time.After(5 * time.Second):
		t.Fatal(timeoutMessage)
	}
}

func TestDispatcher_resolveA2ATargetAgentId_NoOwnerNoLocalMatchKeepsRawTarget(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	d.AgentKeyBase.Store("key-A.alice-laptop", "key-A")

	if got := d.resolveA2ATargetAgentId("key-A.alice-laptop", "bob-laptop"); got != "bob-laptop" {
		t.Fatalf("expected unresolved short target to stay raw, got %q", got)
	}
}

func TestDispatcher_resolveA2ATargetAgentId_DottedLogicalTargetUsesLiveKeyScope(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	sourceID := "nfltr-local-loopback.orch-ui-operator-lobby"
	logicalTarget := "host-a.worker-reviewer-1"
	scopedTarget := "nfltr-local-loopback." + logicalTarget
	d.AgentKeyBase.Store(sourceID, "nfltr-local-loopback")
	d.Upstreams.Store(scopedTarget, newTestGRPCStream(scopedTarget, &channelStreamer{
		sendCh: make(chan *pb.TunnelFrame, 1),
		ctx:    t.Context(),
	}, t.Context()))

	if got := d.resolveA2ATargetAgentId(sourceID, logicalTarget); got != scopedTarget {
		t.Fatalf("expected dotted logical target to resolve within the caller key scope, got %q", got)
	}
}

func TestDispatcher_resolveA2ATargetAgentId_UsesInventoryUniqueSuffixForCrossPod(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	d.StreamerIPInventory = &listOnlyInventory{agentIDs: []string{
		"demo-agent.worker-reviewer-1",
		"demo-agent.worker-reviewer-2",
	}}

	if got := d.resolveA2ATargetAgentId("nfltr-relay-orchestrator", "worker-reviewer-1"); got != "demo-agent.worker-reviewer-1" {
		t.Fatalf("expected inventory suffix resolution to canonical worker id, got %q", got)
	}
}

func TestDispatcher_resolveA2ATargetAgentId_InventorySuffixAmbiguousKeepsRaw(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	d.StreamerIPInventory = &listOnlyInventory{agentIDs: []string{
		"alice.worker-reviewer-1",
		"bob.worker-reviewer-1",
	}}

	if got := d.resolveA2ATargetAgentId("nfltr-relay-orchestrator", "worker-reviewer-1"); got != "worker-reviewer-1" {
		t.Fatalf("expected ambiguous inventory suffix to stay raw, got %q", got)
	}
}

func TestDispatcher_P2PSignal_CrossPodAgentTunnel_ExactTarget(t *testing.T) {
	sourceClient, sourceDispatcher, sourceCleanup := newA2ABufConnEnv(t)
	defer sourceCleanup()

	remoteClient, remoteDispatcher, remoteCleanup := newA2ABufConnEnv(t)
	defer remoteCleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sourceDispatcher.PodDialer = &relayOnlyPodDialer{client: remoteClient, ctx: ctx}

	receiverCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "key-B.bob-live",
		"x-auth-method", "apikey",
	))
	receiverTunnel, err := remoteClient.AgentTunnel(receiverCtx)
	if err != nil {
		t.Fatalf("receiver AgentTunnel: %v", err)
	}
	receiver := rpcclient.NewAgent("key-B.bob-live", receiverTunnel)
	awaitUpstreamRegistered(t, remoteDispatcher, "key-B.bob-live")
	defer receiver.Close() //nolint:errcheck

	handlerDone := make(chan error, 1)
	receiver.Handle(func(stream rpcclient.ServerStream) error {
		offer, err := p2p.RecvSignalServer(stream)
		if err != nil {
			handlerDone <- err
			return err
		}
		if offer.Type != p2p.SignalOffer {
			err = errors.New("expected signal offer")
			handlerDone <- err
			return err
		}
		if err := p2p.SendAnswer(stream, &p2p.SignalMsg{
			Session:    offer.Session,
			Candidates: []string{"127.0.0.1:4242"},
		}); err != nil {
			handlerDone <- err
			return err
		}
		handlerDone <- nil
		return nil
	})

	senderCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "key-A.alice-live",
		"x-auth-method", "apikey",
	))
	senderTunnel, err := sourceClient.AgentTunnel(senderCtx)
	if err != nil {
		t.Fatalf("sender AgentTunnel: %v", err)
	}
	sender := rpcclient.NewAgent("key-A.alice-live", senderTunnel)
	defer sender.Close() //nolint:errcheck

	stream, err := p2p.OpenSignalStream(ctx, sender, "key-B.bob-live")
	if err != nil {
		t.Fatalf("OpenSignalStream: %v", err)
	}
	if err := p2p.SendOffer(stream, &p2p.SignalMsg{
		Session:    "session-42",
		Key:        "deadbeef",
		Candidates: []string{"127.0.0.1:3131"},
		Meta:       &p2p.Meta{Op: "send-file"},
	}); err != nil {
		t.Fatalf("SendOffer: %v", err)
	}

	answer, err := p2p.RecvSignal(stream)
	if err != nil {
		t.Fatalf("RecvSignal: %v", err)
	}
	if answer.Type != p2p.SignalAnswer {
		t.Fatalf("expected signal answer, got %q", answer.Type)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	select {
	case err := <-handlerDone:
		if err != nil {
			t.Fatalf("receiver handler returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("receiver handler did not complete")
	}
}
