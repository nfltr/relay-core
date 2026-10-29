// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package broker is the relay's in-process message broker: topics, one
// producer per topic, exclusive or shared subscriptions, and acknowledged
// messages (NewInMemoryMessageBroker), plus a cache of producers by key
// (Producers).
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
// Close or Unsubscribe.
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
// messages.
const ConsumerTypeExclusive ConsumerType = 0

// ConsumerTypeShared is a subscription that shares the topic's messages
// with other shared subscriptions.
const ConsumerTypeShared ConsumerType = 1

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
