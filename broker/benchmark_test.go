// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// --- DecodeBrokerMessage benchmarks ---

func BenchmarkDecodeBrokerMessage_InMemory(b *testing.B) {
	original := &pb.HTTPProxyResponse{
		Timestamps: &pb.Timestamps{ResponseAtAgent: 12345},
	}
	msg := &testBrokerMessage{raw: original}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBrokerMessage(msg, &pb.HTTPProxyResponse{})
	}
}

func BenchmarkDecodeBrokerMessage_Bytes(b *testing.B) {
	original := &pb.HTTPProxyResponse{
		Timestamps: &pb.Timestamps{ResponseAtAgent: 12345},
	}
	bytes, _ := proto.Marshal(original)
	msg := &testBrokerMessage{payload: bytes}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeBrokerMessage(msg, &pb.HTTPProxyResponse{})
	}
}

// --- SendProtoToBroker benchmarks ---

// --- Producers cache benchmark ---

func BenchmarkProducers_Get_Hit(b *testing.B) {
	broker := NewInMemoryMessageBroker()
	_ = broker.CreateTopic("bench-topic", 0, true)

	producers := NewProducers[string](100, 5*time.Minute, nil)
	opts := ProducerOptions{Topic: "bench-topic"}
	// Pre-populate
	_, _, _ = producers.Get("bench-key", broker, opts)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = producers.Get("bench-key", broker, opts)
	}
}

func BenchmarkProducers_Get_Miss(b *testing.B) {
	broker := NewInMemoryMessageBroker()
	_ = broker.CreateTopic("bench-topic", 0, true)

	producers := NewProducers[string](1000, 5*time.Minute, nil)
	opts := ProducerOptions{Topic: "bench-topic"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, _, _ = producers.Get(key, broker, opts)
	}
}

// --- InMemoryBroker throughput benchmark ---

func BenchmarkInMemoryBroker_Throughput(b *testing.B) {
	mb := NewInMemoryMessageBroker()
	_ = mb.CreateTopic("bench-throughput", 0, true)

	producer, _ := mb.CreateProducer(ProducerOptions{Topic: "bench-throughput"})
	consumer, _ := mb.Subscribe(ConsumerOptions{
		Topic:            "bench-throughput",
		SubscriptionName: "bench-sub",
	})

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Send in goroutine because inMemory Send blocks until subscriber reads
		go func() {
			_, _ = producer.SendBytes(ctx, []byte("payload"))
		}()
		<-consumer.Chan()
	}
}
