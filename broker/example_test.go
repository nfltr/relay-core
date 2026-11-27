// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker_test

import (
	"context"
	"fmt"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// ExampleNewInMemoryMessageBroker demonstrates creating an in-memory broker,
// producing, and consuming a message.
func ExampleNewInMemoryMessageBroker() {
	mb := broker.NewInMemoryMessageBroker()

	// Create a topic
	_ = mb.CreateTopic("example-topic", 0, true)

	// Create a producer
	producer, _ := mb.CreateProducer(broker.ProducerOptions{
		Topic: "example-topic",
	})

	// Create a consumer
	consumer, _ := mb.Subscribe(broker.ConsumerOptions{
		Topic:            "example-topic",
		SubscriptionName: "example-sub",
	})

	// A consumer receives what is sent from Subscribe on.
	msgs := consumer.Chan()

	// Send a message (in a goroutine because in-memory send is synchronous)
	go func() {
		_, _ = producer.SendBytes(context.Background(), []byte("hello world"))
	}()

	// Receive
	msg := <-msgs
	fmt.Println(string(msg.Payload()))

	producer.Close()
	consumer.Close()
	// Output: hello world
}

// ExampleDecodeBrokerMessage shows decoding a proto message from broker bytes.
func ExampleDecodeBrokerMessage() {
	// Simulate receiving a proto-encoded message
	original := &pb.HTTPProxyResponse{
		Timestamps: &pb.Timestamps{
			ResponseAtAgent: 99,
		},
	}
	payload, _ := proto.Marshal(original)

	mb := broker.NewInMemoryMessageBroker()
	_ = mb.CreateTopic("decode-topic", 0, true)

	producer, _ := mb.CreateProducer(broker.ProducerOptions{Topic: "decode-topic"})
	consumer, _ := mb.Subscribe(broker.ConsumerOptions{
		Topic:            "decode-topic",
		SubscriptionName: "decode-sub",
	})

	msgs := consumer.Chan()
	go func() {
		_, _ = producer.SendBytes(context.Background(), payload)
	}()
	msg := <-msgs

	decoded, err := broker.DecodeBrokerMessage(msg, &pb.HTTPProxyResponse{})
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(decoded.Timestamps.ResponseAtAgent)
	// Output: 99
}
