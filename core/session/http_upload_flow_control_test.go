// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/wire/chunking"
)

// An upload chunk asks for acks (chunking.UploadWindow): the agent gets the
// ask and acks chunks itself; the relay never acks for it (lane LR8 deleted
// the relay-side acks for agents that read bodies whole).
func TestRelayRequestsToAgentPassesUploadAckAsk(t *testing.T) {
	for _, tc := range []struct{ name string }{{"streaming agent"}} {
		t.Run(tc.name, func(t *testing.T) {
			d, cancel := newTestDispatcher(t)
			defer cancel()
			defer viper.Reset()
			pod := d.podIDTopicProducer.(*fake.TestProducer)

			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			sent := make(chan *pb.HTTPProxyRequest, 10)
			stream := NewHTTPStream("agent-up", &mockHTTPStreamerCapture{sent: sent}, nil, make(chan proto.Message, 1), ticker)

			topic := "test-topic-upload-" + tc.name
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

			hdrs := map[string]*pb.HeaderValueList{chunking.HeaderUploadAcks: {HeaderValues: []string{"1"}}}
			chunking.SetChunkHeaders(hdrs, 3, false)
			chunk := &pb.HTTPProxyRequest{
				Request:    &pb.HTTPRequest{TransactionId: "txn-up", Headers: hdrs, Payload: []byte("part")},
				Timestamps: &pb.Timestamps{},
				Routing:    &pb.RoutingInfo{AgentId: "agent-up"},
			}
			data, _ := proto.Marshal(chunk)
			if _, err := producer.SendBytes(context.Background(), data); err != nil {
				t.Fatal(err)
			}
			var got *pb.HTTPProxyRequest
			select {
			case got = <-sent:
			case <-time.After(5 * time.Second):
				t.Fatal("chunk not sent to the agent")
			}
			stop()
			<-done

			if !chunking.WantsUploadAcks(got.Request.Headers) {
				t.Fatal("agent did not get the upload ack ask")
			}
			if ack, _ := pod.LastData.(*pb.HTTPProxyResponse); ack != nil {
				t.Fatalf("relay acked for the agent: %v", ack)
			}
		})
	}
}

// Only an ack frame is taken for an ack: a response whose backend set the
// ack header still reaches the client.
func TestIsUploadAckIgnoresResponsesCarryingTheHeader(t *testing.T) {
	if !IsUploadAck(chunking.AckResponse("t", 2).Response) {
		t.Fatal("AckResponse is not an ack")
	}
	forged := &pb.HTTPResponse{
		TransactionId: "t",
		StatusCode:    200,
		Headers:       map[string]*pb.HeaderValueList{chunking.HeaderAck: {HeaderValues: []string{"1"}}},
	}
	if IsUploadAck(forged) {
		t.Fatal("a backend response with the ack header was taken for an ack")
	}
}
