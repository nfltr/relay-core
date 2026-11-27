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
	// ErrNoConsumer: the topic had no consumer to take the message (none
	// subscribed, or it closed before taking it): it was not delivered and
	// its sender answers for it.
	ErrNoConsumer = errors.New("no consumer received the message")
)

// consumerBuffer is how many sent messages a consumer holds that its
// reader has not taken yet.
const consumerBuffer = 64

type inMemoryMessageBroker struct {
	// mu orders the lifecycle of topics, producers and consumers (create,
	// subscribe, close, broker Close), never a send: sends take no broker
	// lock.
	mu                sync.Mutex
	topicAutoCreation bool
	topics            *util.SyncMap[string, *inMemoryTopic]
	producers         *util.SyncMap[string, *inMemoryProducer]
	closed            atomic.Bool
}

// A topic lives while its consumer or its producer holds it: when the last
// of them closes, the broker forgets it (an agent's topic, named by its id,
// leaves nothing behind its session), and a later CreateTopic makes it
// again. A topic CreateTopic made that nobody joined stays until broker
// Close.
type inMemoryTopic struct {
	broker   *inMemoryMessageBroker
	name     string
	consumer atomic.Pointer[inMemoryConsumer] // set and cleared under broker.mu
}

type inMemoryConsumer struct {
	// mu: a send holds it shared while it puts a message on messages;
	// Close holds it exclusive to close messages, so no send is ever on a
	// closed channel.
	mu        sync.RWMutex
	messages  chan Message
	done      chan struct{} // closed first by Close: wakes a blocked send
	closeOnce sync.Once
	topic     *inMemoryTopic
}

type inMemoryProducer struct {
	topic  *inMemoryTopic
	closed atomic.Bool
}

// NewInMemoryMessageBroker returns the in-process broker; its contract is
// in the package doc. It starts no goroutine.
func NewInMemoryMessageBroker() FullBroker {
	return &inMemoryMessageBroker{
		topics:    &util.SyncMap[string, *inMemoryTopic]{},    // topic -> topic
		producers: &util.SyncMap[string, *inMemoryProducer]{}, // topic -> producer
	}
}

func (m *inMemoryMessageBroker) CreateProducer(options ProducerOptions) (Producer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return nil, ErrBrokerClosed
	}
	if _, ok := m.producers.Load(options.Topic); ok { // a closed one has left (Close)
		return nil, ErrProducerExists
	}
	topic, err := m.topicLocked(options.Topic)
	if err != nil {
		return nil, err
	}
	producer := &inMemoryProducer{topic: topic}
	m.producers.Store(options.Topic, producer)
	return producer, nil
}

func (m *inMemoryMessageBroker) Subscribe(options ConsumerOptions) (Consumer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return nil, ErrBrokerClosed
	}
	topic, err := m.topicLocked(options.Topic)
	if err != nil {
		return nil, err
	}
	if topic.consumer.Load() != nil { // a closed one has left (Close)
		return nil, ErrSubscriptionExists
	}
	consumer := &inMemoryConsumer{
		messages: make(chan Message, consumerBuffer),
		done:     make(chan struct{}),
		topic:    topic,
	}
	topic.consumer.Store(consumer)
	return consumer, nil
}

// topicLocked returns the named topic, made when auto-creation is on.
func (m *inMemoryMessageBroker) topicLocked(name string) (*inMemoryTopic, error) {
	if topic, ok := m.topics.Load(name); ok {
		return topic, nil
	}
	if !m.topicAutoCreation {
		return nil, ErrTopicNotExists
	}
	return m.createTopicLocked(name), nil
}

// forgetLocked forgets topic once neither a consumer nor a producer holds
// it.
func (m *inMemoryMessageBroker) forgetLocked(topic *inMemoryTopic) {
	if topic.consumer.Load() != nil {
		return
	}
	if p, ok := m.producers.Load(topic.name); ok && p.topic == topic {
		return
	}
	m.topics.CompareAndDelete(topic.name, topic)
}

// SetTopicAutoCreation lets Subscribe and CreateProducer make a topic that
// does not exist. The namespace is not used.
func (m *inMemoryMessageBroker) SetTopicAutoCreation(_ string, allow bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return ErrBrokerClosed
	}
	m.topicAutoCreation = allow
	return nil
}

func (m *inMemoryMessageBroker) CreateTopic(completeTopicName string, _ int, suppressAlreadyExistsErr bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() {
		return ErrBrokerClosed
	}
	if _, exists := m.topics.Load(completeTopicName); exists {
		if !suppressAlreadyExistsErr {
			return ErrTopicExists
		}
		return nil
	}
	m.createTopicLocked(completeTopicName)
	return nil
}

func (m *inMemoryMessageBroker) createTopicLocked(name string) *inMemoryTopic {
	topic := &inMemoryTopic{broker: m, name: name}
	m.topics.Store(name, topic)
	return topic
}

// Close closes every producer and consumer and forgets every topic; any
// later call but a Close fails with ErrBrokerClosed.
func (m *inMemoryMessageBroker) Close() {
	m.mu.Lock()
	if m.closed.Swap(true) {
		m.mu.Unlock()
		return
	}
	producers, topics := m.producers, m.topics
	m.producers = &util.SyncMap[string, *inMemoryProducer]{}
	m.topics = &util.SyncMap[string, *inMemoryTopic]{}
	m.mu.Unlock()
	// Closed outside mu: their Close takes it (and finds them gone).
	producers.Range(func(_ string, p *inMemoryProducer) bool {
		p.Close()
		return true
	})
	topics.Range(func(_ string, t *inMemoryTopic) bool {
		if c := t.consumer.Load(); c != nil {
			c.Close()
		}
		return true
	})
}

func (p *inMemoryProducer) send(ctx context.Context, msg *message) (string, error) {
	if p.closed.Load() {
		return "", ErrProducerClosed
	}
	c := p.topic.consumer.Load()
	if c == nil {
		return "", ErrNoConsumer
	}
	return "", c.put(ctx, msg)
}

func (p *inMemoryProducer) SendBytes(ctx context.Context, data []byte) (string, error) {
	return p.send(ctx, &message{payload: data})
}

func (p *inMemoryProducer) SendProto(ctx context.Context, msg proto.Message) (string, error) {
	return p.send(ctx, &message{raw: msg})
}

func (p *inMemoryProducer) Close() {
	if p.closed.Swap(true) {
		return
	}
	m := p.topic.broker
	m.mu.Lock()
	defer m.mu.Unlock()
	m.producers.CompareAndDelete(p.topic.name, p)
	m.forgetLocked(p.topic)
}

// put hands msg to the consumer: nil once it is on the consumer's
// channel; ErrNoConsumer when the consumer closed first; ctx's error
// when ctx ended first while the channel was full.
func (c *inMemoryConsumer) put(ctx context.Context, msg Message) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	select {
	case <-c.done:
		return ErrNoConsumer
	default:
	}
	if err := ctx.Err(); err != nil { // an ended send delivers nothing
		return err
	}
	select {
	case c.messages <- msg:
		return nil
	case <-c.done:
		return ErrNoConsumer
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *inMemoryConsumer) Ack(msg Message) error {
	return msg.Ack()
}

// Chan returns the consumer's messages: every message a send put there
// since Subscribe, in the order the sends put them. It is closed by Close,
// after the messages already on it.
func (c *inMemoryConsumer) Chan() <-chan Message {
	return c.messages
}

// Close leaves the topic: a send blocked on this consumer returns
// ErrNoConsumer, later sends find no consumer, and the topic is free for
// a new subscription. Messages already on Chan stay readable.
func (c *inMemoryConsumer) Close() {
	c.closeOnce.Do(func() {
		close(c.done) // wakes a send blocked on a full channel
		c.mu.Lock()   // waits for the sends in put to return
		close(c.messages)
		c.mu.Unlock()
		m := c.topic.broker
		m.mu.Lock()
		defer m.mu.Unlock()
		c.topic.consumer.CompareAndSwap(c, nil)
		m.forgetLocked(c.topic)
	})
}

// Unsubscribe is Close: the in-memory broker keeps no subscription past
// its consumer.
func (c *inMemoryConsumer) Unsubscribe() error {
	c.Close()
	return nil
}
