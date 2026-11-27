// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package broker is the relay's in-process message broker
// (NewInMemoryMessageBroker), plus a cache of producers by key (Producers).
//
// The in-memory broker's contract:
//
//   - A topic is made by CreateTopic (ErrTopicExists when it exists, unless
//     suppressed), or by Subscribe and CreateProducer when auto-creation is
//     on (else they fail with ErrTopicNotExists). It has at most one
//     consumer and at most one producer. When the last of them closes, the
//     broker forgets the topic, and a later join needs it made again: an
//     agent's topic, named by its id, leaves nothing behind its session.
//   - Subscribe joins a topic as its one consumer; while one is open,
//     another fails with ErrSubscriptionExists, so of two at once exactly
//     one wins. The consumer's Type and SubscriptionName do not change
//     that. Once it closes, a new one may join (the same name included).
//   - CreateProducer makes the topic's producer; while one is open another
//     fails with ErrProducerExists.
//   - A send (SendBytes, SendProto) returns nil once the message is on the
//     consumer's channel, which holds 64 messages. When it is full the send
//     waits until the consumer takes one, the consumer closes, or the
//     send's ctx ends. A message not delivered is never silent: the send
//     fails with ErrNoConsumer (no consumer, or it closed first), ctx's
//     error, or ErrProducerClosed. Messages from one sender arrive in the
//     order sent; sends from several goroutines are not ordered.
//   - A consumer receives every message sent while it is subscribed, from
//     Subscribe on (Chan may be taken later). Close (and Unsubscribe, the
//     same) is idempotent, wakes a waiting send (ErrNoConsumer), and closes
//     the channel after the messages already on it, which stay readable:
//     the closing side answers for them.
//   - Producer Close is idempotent; its later sends fail with
//     ErrProducerClosed.
//   - Broker Close closes every producer and consumer and forgets every
//     topic; later calls fail with ErrBrokerClosed.
//   - All of it is safe for concurrent use; the broker starts no goroutine
//     and no send takes a broker-wide lock.
package broker

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// Message is a message a Consumer received: its bytes, the proto message it
// carries when it was sent as one (SendProto), and its acknowledgement.
type Message interface {
	Payload() []byte
	RawObject() proto.Message
	Ack() error
}

// Consumer is a subscription to a topic: its messages arrive on Chan until
// Close or Unsubscribe (the package doc has the in-memory contract).
type Consumer interface {
	// Ack acknowledges a message. Delegates to msg.Ack() — provided as a
	// convenience so callers can use the traditional consumer.Ack(msg) style.
	Ack(Message) error
	Chan() <-chan Message
	Close()
	Unsubscribe() error
}

// ConsumerType is how a subscription shares a topic's messages.
type ConsumerType int

// ConsumerTypeExclusive is a subscription that alone receives the topic's
// messages: the only kind the in-memory broker has.
const ConsumerTypeExclusive ConsumerType = 0

// ConsumerOptions name the topic and subscription a Subscribe joins.
type ConsumerOptions struct {
	Topic            string
	Type             ConsumerType
	SubscriptionName string
}

// ProducerOptions name the topic a producer sends on.
type ProducerOptions struct {
	Topic string
}

// Producer sends messages on one topic and returns each message's id.
type Producer interface {
	SendBytes(ctx context.Context, data []byte) (string, error)
	SendProto(ctx context.Context, msg proto.Message) (string, error)
	Close()
}

// MessageBroker provides runtime broker operations: producing and consuming messages.
type MessageBroker interface {
	CreateProducer(options ProducerOptions) (Producer, error)
	Subscribe(options ConsumerOptions) (Consumer, error)
	CreateTopic(completeTopicName string, partitions int, suppressAlreadyExistsErr bool) error
	Close()
}

// FullBroker is the in-process broker: MessageBroker plus topic
// auto-creation, which tests switch on.
type FullBroker interface {
	MessageBroker
	SetTopicAutoCreation(namespace string, allow bool) error
}

type message struct {
	payload []byte
	raw     proto.Message
}

func (m *message) Payload() []byte {
	return m.payload
}

func (m *message) RawObject() proto.Message {
	return m.raw
}

func (m *message) Ack() error { return nil }
