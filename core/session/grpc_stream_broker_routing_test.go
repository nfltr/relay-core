// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

func TestRouteResponseToBroker_InMemoryBroker_SendError(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	// Use an errProducer that will fail on Send
	badProducer := &errProducer{sendErr: errors.New("send failed")}
	d := NewDispatcher(ctx, fake.TestLogger(), b, badProducer, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
	})

	input := &pb.HTTPProxyResponse{
		Routing: &pb.RoutingInfo{AgentId: "agentID-respond-err"},
		Response: &pb.HTTPResponse{
			TransactionId: "txn-respond-err",
		},
	}

	err := d.routeResponseToBroker(ctx, input)
	if err == nil {
		t.Fatal("expected error from routeResponseToBroker with failing producer")
	}
	if !strings.Contains(err.Error(), "send failed") {
		t.Errorf("expected 'send failed' error, got: %v", err)
	}
}

func TestSend_HeartbeatSendFailure_CacheDeleteFailure(t *testing.T) {
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	ctx := t.Context()

	errInv := &errorIPInventory{
		deleteErr: errors.New("cache delete failed"),
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, &fake.TestProducer{}, errInv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    50 * time.Millisecond,
		RecvBufferSize:           10,
	})

	// Create an agent stream with a send that fails
	failingStream := &mockConnectHTTPServer{
		ctx:     context.Background(),
		sendErr: errors.New("stream send failed"),
	}

	agentID := "agentID-hb-cache-fail"
	agentStream := NewHTTPStream(agentID, failingStream, nil, make(chan proto.Message, 10), time.NewTicker(5*time.Second))

	topicName := agentID
	_ = b.CreateTopic(topicName, 0, true)
	consumer, _ := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: agentID,
		Type:             broker.ConsumerTypeExclusive,
	})

	err := d.relayRequestsToAgent(ctx, agentStream, consumer)
	if err == nil {
		t.Fatal("expected error from send with heartbeat failure")
	}
}

func TestSend_AckError(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	ctx := t.Context()

	d := NewDispatcher(ctx, fake.TestLogger(), b, nil, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    time.Hour, // won't fire heartbeat ticker
		RecvBufferSize:           10,
	})

	// Build a valid serialized HTTPProxyRequest for the broker message
	output := &pb.HTTPProxyRequest{
		Request: &pb.HTTPRequest{
			TransactionId: "txn-ack-err",
			Headers:       map[string]*pb.HeaderValueList{},
			Payload:       []byte("body"),
		},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-ack-err"},
	}
	data, err := proto.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}

	// Create a mock consumer with Ack error on the message
	ch := make(chan broker.Message, 1)
	ch <- &stubMessage{payload: data, ackErr: errors.New("ack failed")}
	mc := &mockConsumer{
		ch: ch,
	}

	// Create a mock agent stream that succeeds on Send
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	mockStream := &mockConnectHTTPServer{
		ctx: context.Background(),
	}
	agentStream := NewHTTPStream("agentID-ack-err", mockStream, nil, heartbeats, ticker)

	err = d.relayRequestsToAgent(ctx, agentStream, mc)
	if err == nil || !strings.Contains(err.Error(), "ack") {
		t.Fatalf("expected ack error, got: %v", err)
	}
}

func TestSend_HeartbeatSendFailure_DeleteIPsError(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	ctx := t.Context()

	errInv := &errorIPInventory{
		deleteErr: errors.New("cache delete failed"),
	}

	d := NewDispatcher(ctx, fake.TestLogger(), b, nil, errInv, directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    50 * time.Millisecond, // heartbeat fires quickly
		RecvBufferSize:           10,
	})

	// Agent stream with a mock that fails on Send
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	mockStream := &mockConnectHTTPServer{
		ctx:     context.Background(),
		sendErr: errors.New("send failed"),
	}
	agentStream := NewHTTPStream("agentID-hb-del", mockStream, nil, heartbeats, ticker)

	ch := make(chan broker.Message) // empty, won't deliver messages
	mc := &mockConsumer{ch: ch}

	err := d.relayRequestsToAgent(ctx, agentStream, mc)
	if err == nil || !strings.Contains(err.Error(), "send failed") {
		t.Fatalf("expected 'send failed', got: %v", err)
	}
}

func TestSend_HeartbeatSuccessPath(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	ctx, cancel := context.WithCancel(context.Background())

	d := NewDispatcher(ctx, fake.TestLogger(), b, nil, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    50 * time.Millisecond, // short heartbeat
		RecvBufferSize:           10,
	})

	// Create a mock agent stream that accepts sends
	heartbeats := make(chan proto.Message, 10)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()

	mockStream := &mockConnectHTTPServer{
		ctx: context.Background(),
	}
	agentStream := NewHTTPStream("agentID-hb-ok", mockStream, nil, heartbeats, ticker)

	// Create a consumer with empty channel (no messages to process)
	mc := &mockConsumer{
		ch: make(chan broker.Message), // no messages
	}

	// Cancel context after heartbeat has time to fire at least once
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()

	err := d.relayRequestsToAgent(ctx, agentStream, mc)
	if err == nil || err.Error() != "context canceled" {
		t.Fatalf("expected context canceled, got: %v", err)
	}
}

type errProducer struct {
	sendErr error
}

func (p *errProducer) SendBytes(_ context.Context, _ []byte) (string, error) {
	return "", p.sendErr
}

func (p *errProducer) SendProto(_ context.Context, _ proto.Message) (string, error) {
	return "", p.sendErr
}

func (p *errProducer) Close() {}

// mockConsumer implements broker.Consumer for testing.
type mockConsumer struct {
	ch       chan broker.Message
	unsubErr error
}

func (m *mockConsumer) Ack(msg broker.Message) error { return msg.Ack() }

func (m *mockConsumer) Chan() <-chan broker.Message { return m.ch }

func (m *mockConsumer) Close() {}

func (m *mockConsumer) Unsubscribe() error { return m.unsubErr }

// stubMessage is a simple broker.Message for tests.
type stubMessage struct {
	payload []byte
	ackErr  error
}

func (s *stubMessage) Payload() []byte { return s.payload }

func (s *stubMessage) RawObject() proto.Message { return nil }

func (s *stubMessage) Ack() error { return s.ackErr }
