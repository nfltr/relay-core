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
