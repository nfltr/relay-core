// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"testing"

	"github.com/nfltr/relay/proto/pb"
	"github.com/stretchr/testify/require"
)

func TestInMemoryMessageBroker_CreateProducer(t *testing.T) {
	broker := NewInMemoryMessageBroker()

	// Test creating a new producer
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "test-topic"})
	require.Error(t, err)
	require.Nil(t, producer)

	// Test creating a producer for an existing topic
	producer, err = broker.CreateProducer(ProducerOptions{Topic: "test-topic"})
	require.EqualError(t, err, ErrTopicNotExists.Error())
	require.Nil(t, producer)

	// Test creating a producer for a non-existing topic with auto-creation enabled
	err = broker.SetTopicAutoCreation("test-namespace", true)
	require.NoError(t, err)
	producer, err = broker.CreateProducer(ProducerOptions{Topic: "new-topic"})
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Test creating a producer for a non-existing topic with auto-creation disabled
	err = broker.SetTopicAutoCreation("test-namespace", false)
	require.NoError(t, err)
	producer, err = broker.CreateProducer(ProducerOptions{Topic: "new-topic"})
	require.EqualError(t, err, ErrProducerExists.Error())
	require.Nil(t, producer)
}

func TestInMemoryMessageBroker_Subscribe(t *testing.T) {
	broker := NewInMemoryMessageBroker()

	// Test subscribing to an existing topic
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "test-topic", SubscriptionName: "test-subscription"})
	require.EqualError(t, err, ErrTopicNotExists.Error())
	require.Nil(t, consumer)

	// Test subscribing to a non-existing topic with auto-creation enabled
	err = broker.SetTopicAutoCreation("test-namespace", true)
	require.NoError(t, err)
	consumer, err = broker.Subscribe(ConsumerOptions{Topic: "new-topic", SubscriptionName: "test-subscription"})
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Test subscribing to a non-existing topic with auto-creation disabled
	err = broker.SetTopicAutoCreation("test-namespace", false)
	require.NoError(t, err)
	consumer, err = broker.Subscribe(ConsumerOptions{Topic: "new-topic-x", SubscriptionName: "test-subscription"})
	require.EqualError(t, err, ErrTopicNotExists.Error())
	require.Nil(t, consumer)

	// Test subscribing with an existing subscription
	err = broker.SetTopicAutoCreation("test-namespace", true)
	require.NoError(t, err)
	consumer, err = broker.Subscribe(ConsumerOptions{Topic: "new-topic", SubscriptionName: "test-subscription"})
	require.Error(t, err, ErrSubscriptionExists.Error())
	require.Nil(t, consumer)
}

func TestInMemoryMessageBroker_CreateTopic(t *testing.T) {
	broker := NewInMemoryMessageBroker()

	// Test creating a new topic
	err := broker.CreateTopic("test-topic", 1, false)
	require.NoError(t, err)

	// Test creating an existing topic
	err = broker.CreateTopic("test-topic", 1, false)
	require.EqualError(t, err, ErrTopicExists.Error())

	// Test creating an existing topic with suppressAlreadyExistsErr set to true
	err = broker.CreateTopic("test-topic", 1, true)
	require.NoError(t, err)
}

func TestInMemoryMessageBroker_Close(t *testing.T) {
	broker := NewInMemoryMessageBroker()

	// Test closing the broker
	broker.Close()

	err := broker.SetTopicAutoCreation("test-namespace", true)
	require.Error(t, err, ErrBrokerClosed.Error())
	// Test closing the broker with existing producers and consumers
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "test-topic"})
	require.Error(t, err, ErrBrokerClosed.Error())
	require.Nil(t, producer)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "test-topic", SubscriptionName: "test-subscription"})
	require.Error(t, err, ErrBrokerClosed.Error())
	require.Nil(t, consumer)

	broker.Close()
}

func TestInMemoryProducer_Send(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	err := broker.CreateTopic("test-topic", 1, false)
	require.NoError(t, err)
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "test-topic"})
	require.NoError(t, err)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "test-topic", SubscriptionName: "test-subscription"})
	require.NoError(t, err)
	msgs := consumer.Chan()
	ctx := context.Background()
	// Receive in a goroutine and pass back via channel (Send blocks on unbuffered channel)
	result := make(chan Message, 1)
	go func() {
		result <- <-msgs
	}()
	// Send the message
	_, err = producer.SendBytes(ctx, []byte("test-message"))
	require.NoError(t, err)
	// Verify on the test goroutine (require must not be called from non-test goroutines)
	msg := <-result
	require.Equal(t, msg.Payload(), []byte("test-message"))

	// Test sending a message to a closed producer
	producer.Close()
	_, err = producer.SendBytes(ctx, []byte("test-message"))
	require.Error(t, err, ErrProducerClosed.Error())

	// Test sending a message to a closed consumer
	consumer.Close()
	_, err = producer.SendBytes(ctx, []byte("test-message"))
	require.Error(t, err, ErrProducerClosed.Error())
}

func TestInMemoryConsumer_Close(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	err := broker.CreateTopic("test-topic", 1, true)
	require.NoError(t, err)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "test-topic", SubscriptionName: "test-subscription"})
	require.NoError(t, err)
	// Test closing the consumer
	consumer.Close()
	// Test closing the consumer multiple times
	consumer.Close()

	_, err = broker.Subscribe(ConsumerOptions{Topic: "test-topic", SubscriptionName: "test-subscription-x", Type: ConsumerTypeExclusive})
	require.Error(t, err)
}

func BenchmarkInMemoryProducer_Send(b *testing.B) {
	broker := NewInMemoryMessageBroker()
	err := broker.CreateTopic("test-topic", 1, false)
	require.Nil(b, err)
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "test-topic"})
	require.Nil(b, err)
	defer producer.Close()
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "test-topic", SubscriptionName: "test-subscription"})
	require.Nil(b, err)
	defer consumer.Close()
	msgs := consumer.Chan()

	ctx := context.Background()
	// Producer runs in a goroutine; errors are collected via channel
	// to avoid calling b.Fatal from a non-benchmark goroutine.
	errCh := make(chan error, b.N)
	go func() {
		defer close(errCh)
		for i := 0; i < b.N; i++ {
			if _, err := producer.SendBytes(ctx, []byte("test-message")); err != nil {
				errCh <- err
				return
			}
		}
	}()

	for i := 0; i < b.N; i++ {
		msg := <-msgs
		require.Equal(b, msg.Payload(), []byte("test-message"))
	}
	for err := range errCh {
		b.Fatalf("producer send error: %v", err)
	}
}

// --- Additional tests to close coverage gaps ---

func TestMessage_RawObject(t *testing.T) {
	original := &pb.HTTPProxyResponse{Timestamps: &pb.Timestamps{ResponseAtAgent: 1}}
	m := &message{payload: []byte("p"), raw: original}
	require.Equal(t, []byte("p"), m.Payload())
	require.Equal(t, original, m.RawObject())
}

func TestMessage_RawObjectNil(t *testing.T) {
	m := &message{payload: []byte("p")}
	require.Nil(t, m.RawObject())
}

func TestInMemoryMessage_Ack(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	require.NoError(t, broker.CreateTopic("t", 1, false))
	_, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	msg := &message{payload: []byte("x")}
	require.NoError(t, msg.Ack())
}

func TestInMemoryProducer_Send_RawObjectBranch(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	require.NoError(t, broker.CreateTopic("t", 1, false))
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "t"})
	require.NoError(t, err)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	msgs := consumer.Chan()

	original := &pb.HTTPProxyResponse{Timestamps: &pb.Timestamps{ResponseAtAgent: 42}}
	// Receive in goroutine; verify on test goroutine
	result := make(chan Message, 1)
	go func() {
		result <- <-msgs
	}()
	_, err = producer.SendProto(context.Background(), original)
	require.NoError(t, err)
	msg := <-result
	require.Nil(t, msg.Payload())
	require.Equal(t, original, msg.RawObject())
}

func TestInMemoryProducer_Send_ClosedConsumerCleanup(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	require.NoError(t, broker.CreateTopic("t", 1, false))
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "t"})
	require.NoError(t, err)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	// Start receiving then close
	_ = consumer.Chan()
	consumer.Close()

	// A closed consumer takes nothing more: the sender is told
	_, err = producer.SendBytes(context.Background(), []byte("msg"))
	require.ErrorIs(t, err, ErrNoConsumer)
}

func TestInMemoryMessageBroker_CreateProducer_ClosedProducerReuse(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	require.NoError(t, broker.CreateTopic("t", 1, false))
	_, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"}) // holds the topic
	require.NoError(t, err)
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "t"})
	require.NoError(t, err)
	producer.Close()

	// Now create again — should reuse after deleting closed producer
	producer2, err := broker.CreateProducer(ProducerOptions{Topic: "t"})
	require.NoError(t, err)
	require.NotNil(t, producer2)
}

func TestInMemoryMessageBroker_Subscribe_ClosedSubscriptionReuse(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	require.NoError(t, broker.CreateTopic("t", 1, false))
	_, err := broker.CreateProducer(ProducerOptions{Topic: "t"}) // holds the topic
	require.NoError(t, err)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	consumer.Close()

	// Resubscribe should work after closing
	consumer2, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	require.NotNil(t, consumer2)
}

func TestInMemoryMessageBroker_CloseWithActiveProducersAndConsumers(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	require.NoError(t, broker.CreateTopic("t", 1, false))
	producer, err := broker.CreateProducer(ProducerOptions{Topic: "t"})
	require.NoError(t, err)
	consumer, err := broker.Subscribe(ConsumerOptions{Topic: "t", SubscriptionName: "s"})
	require.NoError(t, err)
	_ = consumer.Chan()

	broker.Close()

	// Producer and consumer should be closed
	_, err = producer.SendBytes(context.Background(), []byte("msg"))
	require.Error(t, err)
}

func TestInMemoryMessageBroker_CreateTopic_WhenClosed(t *testing.T) {
	broker := NewInMemoryMessageBroker()
	broker.Close()
	err := broker.CreateTopic("t", 1, false)
	require.ErrorIs(t, err, ErrBrokerClosed)
}
