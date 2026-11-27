// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

// A flow-control frame (cancel, ack) reaches the agent: every agent the
// relay admits reads them (the protocol v1 Hello cut postdates flow
// control; lane LR8 deleted the branch that dropped them for older agents).
func TestRelayRequestsToAgentSendsFlowControlFrames(t *testing.T) {
	for _, tc := range []struct{ name string }{{"agent"}} {
		t.Run(tc.name, func(t *testing.T) {
			d, cancel := newTestDispatcher(t)
			defer cancel()
			defer viper.Reset()

			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			sent := make(chan *pb.HTTPProxyRequest, 10)
			stream := NewHTTPStream("agent-fc", &mockHTTPStreamerCapture{sent: sent}, nil, make(chan proto.Message, 1), ticker)

			topic := "test-topic-flow-" + tc.name
			_ = d.broker.CreateTopic(topic, 1, true)
			consumer, err := d.broker.Subscribe(broker.ConsumerOptions{Topic: topic, SubscriptionName: "sub", Type: broker.ConsumerTypeExclusive})
			if err != nil {
				t.Fatal(err)
			}
			producer, err := d.broker.CreateProducer(broker.ProducerOptions{Topic: topic})
			if err != nil {
				t.Fatal(err)
			}

			ctx, stop := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- d.relayRequestsToAgent(ctx, stream, consumer) }()
			time.Sleep(50 * time.Millisecond) // consumer.Chan() subscribed
			for _, frame := range []*pb.HTTPProxyRequest{
				chunking.CancelRequest("agent-fc", "txn-1"),
				chunking.AckRequest("agent-fc", "txn-1", 0),
			} {
				data, _ := proto.Marshal(frame)
				if _, err := producer.SendBytes(context.Background(), data); err != nil {
					t.Fatal(err)
				}
			}
			time.Sleep(100 * time.Millisecond)
			stop()
			<-done

			if got := len(sent); got != 2 {
				t.Fatalf("sent %d flow-control frames to the agent, want 2", got)
			}
		})
	}
}
