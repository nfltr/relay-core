// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/nfltr/relay-core/platform/util"
	"google.golang.org/protobuf/proto"
)

// Errors of the in-memory broker.
var (
	ErrBrokerClosed       = errors.New("broker already closed")
	ErrProducerExists     = errors.New("producer already exists for topic")
	ErrProducerClosed     = errors.New("producer already closed")
	ErrTopicExists        = errors.New("topic already exists")
	ErrTopicNotExists     = errors.New("topic not exist")
	ErrSubscriptionExists = errors.New("subscription already exists for topic")
	// ErrNoConsumer: the topic's consumer closed and no other took its
	// place, so the message was not delivered: its sender answers for it.
	// (A topic that never had a consumer still takes messages silently.)
	ErrNoConsumer = errors.New("no consumer received the message")
)

type inMemoryMessageBroker struct {
	namespace         string
	topicAutoCreation bool
	topics            *util.SyncMap[string, *inMemoryTopic]
	producers         *util.SyncMap[string, *inMemoryProducer]
	closed            atomic.Bool
}

type inMemoryTopic struct {
	name        string
	subscribers *util.SyncMap[string, *inMemoryConsumer]
	ended       atomic.Bool // a consumer closed and none subscribed since
}

type inMemoryConsumer struct {
	lock             sync.Mutex
	messages         chan Message
	closed           bool
	closeOnce        sync.Once
	consumerType     ConsumerType
	receiveOnce      sync.Once
	receiving        bool
	subscriptionName string
	topic            *inMemoryTopic
}

type inMemoryProducer struct {
	topic  *inMemoryTopic
	closed atomic.Bool
}

// NewInMemoryMessageBroker creates a new in-memory message broker
// It has no namespace and ttl support
func NewInMemoryMessageBroker() FullBroker {
	return &inMemoryMessageBroker{
		topics:    &util.SyncMap[string, *inMemoryTopic]{},    // topic -> topic
		producers: &util.SyncMap[string, *inMemoryProducer]{}, // topic -> producer
	}
}

func (m *inMemoryMessageBroker) CreateProducer(options ProducerOptions) (Producer, error) {
	if m.closed.Load() {
		return nil, ErrBrokerClosed
	}
	producer, ok := m.producers.Load(options.Topic)
	if ok && !producer.closed.Load() {
		return nil, ErrProducerExists
	}
	if ok && producer.closed.Load() {
		m.producers.Delete(options.Topic)
	}

	if _, exists := m.topics.Load(options.Topic); !exists && m.topicAutoCreation {
		if err := m.CreateTopic(options.Topic, 1, true); err != nil {
			return nil, err
		}
	}
	topic, ok := m.topics.Load(options.Topic)
	if !ok {
		return nil, ErrTopicNotExists
	}

	producer = &inMemoryProducer{
		topic: topic,
	}
	m.producers.Store(options.Topic, producer)

	return producer, nil
}

func (m *inMemoryMessageBroker) Subscribe(options ConsumerOptions) (Consumer, error) {
	if m.closed.Load() {
		return nil, ErrBrokerClosed
	}
	if _, exists := m.topics.Load(options.Topic); !exists && m.topicAutoCreation {
		if err := m.CreateTopic(options.Topic, 1, true); err != nil {
			return nil, err
		}
	}

	topic, ok := m.topics.Load(options.Topic)
	if !ok {
		return nil, ErrTopicNotExists
	}

	subscription, ok := topic.subscribers.Load(options.SubscriptionName)
	if ok && !subscription.closed {
		return nil, ErrSubscriptionExists
	}
	if ok && subscription.closed {
		topic.subscribers.Delete(options.SubscriptionName)
	}
	var sharedConsumers, exclusiveConsumers int
	topic.subscribers.Range(func(_ string, c *inMemoryConsumer) bool {
		switch c.consumerType {
		case ConsumerTypeExclusive:
			exclusiveConsumers++
		case ConsumerTypeShared:
			sharedConsumers++
		}
		return true
	})
	if options.Type == ConsumerTypeExclusive && sharedConsumers+exclusiveConsumers != 0 {
		return nil, ErrSubscriptionExists
	}
	if options.Type == ConsumerTypeShared && exclusiveConsumers != 0 {
		return nil, ErrSubscriptionExists
	}

	consumer := &inMemoryConsumer{
		messages:         make(chan Message, 64),
		consumerType:     options.Type,
		subscriptionName: options.SubscriptionName,
		topic:            topic,
	}
	topic.ended.Store(false)

	topic.subscribers.Store(consumer.subscriptionName, consumer)

	return consumer, nil
}

func (m *inMemoryMessageBroker) SetTopicAutoCreation(namespace string, allow bool) error {
	if m.closed.Load() {
		return ErrBrokerClosed
	}
	m.namespace = namespace
	m.topicAutoCreation = allow
	return nil
}

func (m *inMemoryMessageBroker) CreateTopic(completeTopicName string, partitions int, suppressAlreadyExistsErr bool) error {
	if m.closed.Load() {
		return ErrBrokerClosed
	}
	_, exists := m.topics.Load(completeTopicName)
	if exists && !suppressAlreadyExistsErr {
		return ErrTopicExists
	}
	if !exists {
		topic := &inMemoryTopic{
			name:        completeTopicName,
			subscribers: &util.SyncMap[string, *inMemoryConsumer]{},
		}
		m.topics.Store(completeTopicName, topic)
	}

	return nil
}

func (m *inMemoryMessageBroker) Close() {
	// Set closed first to prevent new operations from racing with teardown.
	if m.closed.Swap(true) {
		return
	}
	m.producers.Range(func(_ string, producer *inMemoryProducer) bool {
		producer.Close()
		return true
	})
	m.topics.Range(func(_ string, topic *inMemoryTopic) bool {
		topic.subscribers.Range(func(_ string, consumer *inMemoryConsumer) bool {
			consumer.Close()
			return true
		})
		return true
	})
	m.producers = &util.SyncMap[string, *inMemoryProducer]{}
	m.topics = &util.SyncMap[string, *inMemoryTopic]{}
}

func (p *inMemoryProducer) sendToSubscribers(ctx context.Context, msg *message) (string, error) {
	if p.closed.Load() {
		return "", ErrProducerClosed
	}
	delivered := false
	p.topic.subscribers.Range(func(_ string, subscriber *inMemoryConsumer) bool {
		subscriber.lock.Lock()
		defer subscriber.lock.Unlock()
		if subscriber.closed {
			p.topic.subscribers.Delete(subscriber.subscriptionName)
			return true
		}
		if !subscriber.receiving {
			return true
		}
		select {
		case <-ctx.Done():
		case subscriber.messages <- msg:
			delivered = true
		}
		return true
	})
	if !delivered && p.topic.ended.Load() {
		return "", ErrNoConsumer
	}
	return "", nil
}

func (p *inMemoryProducer) SendBytes(ctx context.Context, data []byte) (string, error) {
	return p.sendToSubscribers(ctx, &message{payload: data})
}

func (p *inMemoryProducer) SendProto(ctx context.Context, msg proto.Message) (string, error) {
	return p.sendToSubscribers(ctx, &message{raw: msg})
}

func (p *inMemoryProducer) Close() {
	p.closed.Store(true)
}

func (c *inMemoryConsumer) Ack(msg Message) error {
	return msg.Ack()
}

// Chan starts the consumer receiving: a message sent before the first
// Chan call is not delivered to it.
func (c *inMemoryConsumer) Chan() <-chan Message {
	c.receiveOnce.Do(func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		c.receiving = true
	})

	return c.messages
}

func (c *inMemoryConsumer) Close() {
	c.closeOnce.Do(func() {
		c.lock.Lock()
		defer c.lock.Unlock()
		close(c.messages)
		c.closed = true
		if c.topic != nil {
			c.topic.ended.Store(true)
		}
	})
}

func (c *inMemoryConsumer) Unsubscribe() error {
	return nil
}
