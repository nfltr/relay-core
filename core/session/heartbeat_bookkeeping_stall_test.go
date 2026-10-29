// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// TestRelayRequestsToAgentNotHeldByHeartbeatBookkeeping: a heartbeat's
// bookkeeping (OnAgentHeartbeat: the endpoint registry's last_seen_at, a
// SQL write) that hangs, as on a replica healing from a partition, must not
// hold the requests the loop forwards to the agent (soak 2026-10-26: a
// replica answered forwarded ConnectHTTP requests 10-22 s late).
func TestRelayRequestsToAgentNotHeldByHeartbeatBookkeeping(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.HeartbeatSendDuration = 20 * time.Millisecond
	hung, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	d.OnAgentHeartbeat = func(string) {
		select {
		case hung <- struct{}{}:
		default:
		}
		<-release
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	sent := make(chan *pb.HTTPProxyRequest, 16)
	stream := NewHTTPStream("agent-hb", &mockHTTPStreamerCapture{sent: sent}, nil, make(chan proto.Message, 1), ticker)

	topic := "agent-hb"
	_ = d.broker.CreateTopic(topic, 1, true)
	consumer, err := d.broker.Subscribe(broker.ConsumerOptions{Topic: topic, SubscriptionName: topic, Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := d.broker.CreateProducer(broker.ProducerOptions{Topic: topic})
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = d.relayRequestsToAgent(ctx, stream, consumer) }()

	select {
	case <-hung: // the first heartbeat's bookkeeping is hanging
	case <-time.After(2 * time.Second):
		t.Fatal("no heartbeat bookkeeping")
	}
	req := &pb.HTTPProxyRequest{
		Request:    &pb.HTTPRequest{TransactionId: "txn-1"},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "agent-hb"},
	}
	pubCtx, pubCancel := context.WithTimeout(context.Background(), time.Second)
	defer pubCancel()
	if _, err := producer.SendProto(pubCtx, req); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		select {
		case m := <-sent:
			if m.GetRequest().GetTransactionId() == "txn-1" {
				return
			}
		case <-deadline:
			t.Fatal("request not sent to the agent while the heartbeat bookkeeping hangs")
		}
	}
}
