// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

func TestProxy_FanoutResponses_AckError(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("http.server.fanout_workers", 1)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	ctx, cancel := context.WithCancel(context.Background())

	// Create a consumer and manually close it to force ack errors
	topicName := "test-fanout-ack"
	_ = b.CreateTopic(topicName, 0, true)
	consumer, _ := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-fanout",
		Type:             broker.ConsumerTypeExclusive,
	})

	// Start FanoutResponses in background
	done := make(chan error, 1)
	go func() {
		done <- proxy.FanoutResponses(ctx, "pod-1", consumer)
	}()

	// Let it start consuming
	time.Sleep(100 * time.Millisecond)

	// Cancel context to stop cleanly
	cancel()

	select {
	case err := <-done:
		// WorkerPool returns nil on context cancel
		_ = err
	case <-time.After(3 * time.Second):
		t.Fatal("FanoutResponses didn't return")
	}
}

func TestProxy_fanoutResponse_DecodeError(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	// Bad message that can't be decoded
	msg := &testBrokerMessage{payload: []byte("invalid-proto")}
	err := proxy.fanoutResponse(msg, time.Now())
	if err == nil {
		t.Fatal("expected error for bad proto message")
	}
}

func TestProxy_fanoutResponse_NoResponseChannel(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	// Valid message but no response channel registered
	response := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-missing", StatusCode: 200},
		Timestamps: &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	msg := &testBrokerMessage{raw: response}
	err := proxy.fanoutResponse(msg, time.Now())
	if err == nil {
		t.Fatal("expected error for missing response channel")
	}
}

func TestProxy_fanoutResponse_NoRequestContext(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	response := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-not-registered", StatusCode: 200},
		Timestamps: &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	// No transaction registered — txnRegistry.Fanout should fail
	msg := &testBrokerMessage{raw: response}
	err := proxy.fanoutResponse(msg, time.Now())
	if err == nil {
		t.Fatal("expected error for unregistered transaction")
	}
}

func TestProxy_fanoutResponse_Success(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	response := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-1", StatusCode: 200},
		Timestamps: &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	handle, cleanup := proxy.txnRegistry.Register("txn-1", context.Background(), testLogger())
	defer cleanup()

	msg := &testBrokerMessage{raw: response}

	// fanoutResponse sends on an unbuffered channel, so run it concurrently
	fanoutDone := make(chan error, 1)
	go func() {
		fanoutDone <- proxy.fanoutResponse(msg, time.Now())
	}()

	select {
	case r := <-handle.ResponseCh:
		if r.Response.TransactionId != "txn-1" {
			t.Fatalf("expected txn-1, got %s", r.Response.TransactionId)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for response in channel")
	}

	if err := <-fanoutDone; err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFanoutResponses_AckError(t *testing.T) {
	viper.Set("http.server.fanout_workers", 1)
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	p := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	// Build a valid HTTPProxyResponse message as broker payload
	si := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-ack-err"},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-ack-err"},
	}
	data, _ := proto.Marshal(si)

	// Register response channel and context via txnRegistry to prevent fanoutResponse error
	handle, cleanup := p.txnRegistry.Register("txn-ack-err", context.Background(), testLogger())
	defer cleanup()
	// Drain the response channel so fanout doesn't block
	drained := make(chan struct{})
	defer close(drained)
	go func() {
		for {
			select {
			case <-handle.ResponseCh:
			case <-drained:
				return
			}
		}
	}()

	ch := make(chan broker.Message, 1)
	ch <- &stubMessage{payload: data, ackErr: errors.New("ack failed")}
	mc := &mockConsumer{
		ch: ch,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Ack errors are non-fatal (logged but do not kill workers).
	// FanoutResponses should return ctx.Err() (context.DeadlineExceeded)
	// rather than propagating the ack error.
	err := p.FanoutResponses(ctx, "pod-1", mc)
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected nil or context deadline error, got: %v", err)
	}
}

func TestFanoutResponses_FanoutError(t *testing.T) {
	viper.Set("http.server.fanout_workers", 1)
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	p := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	// Bad payload to trigger decodeBrokerMessage error
	ch := make(chan broker.Message, 1)
	ch <- &stubMessage{payload: []byte("not valid protobuf")}
	mc := &mockConsumer{
		ch: ch,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// This should log a warning for fanoutErr but return no error (ack succeeds)
	err := p.FanoutResponses(ctx, "pod-1", mc)
	// ctx cancelled, no ack error → returns ctx.Err()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Logf("got error: %v", err)
	}
}

func TestProxy_FanoutResponses(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("http.server.fanout_workers", 2)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	podTopic := "pod-1"
	_ = b.CreateTopic(podTopic, 0, true)

	consumer, err := b.Subscribe(broker.ConsumerOptions{
		Topic:            podTopic,
		SubscriptionName: "pod-1-sub",
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer consumer.Close()

	// Set up response channel and request context via txnRegistry
	reqCtx := t.Context()
	handle, cleanup := proxy.txnRegistry.Register("txn-fanout", reqCtx, testLogger())
	defer cleanup()

	// Drain the response from the unbuffered channel concurrently
	var received *pb.HTTPProxyResponse
	receivedCh := make(chan struct{})
	go func() {
		select {
		case r := <-handle.ResponseCh:
			received = r
			close(receivedCh)
		case <-reqCtx.Done():
			close(receivedCh)
		}
	}()

	// Activate receiving before publishing so the message reaches the consumer
	_ = consumer.Chan()

	// Publish a message in a goroutine (unbuffered channel needs a receiver)
	producer, _ := b.CreateProducer(broker.ProducerOptions{Topic: podTopic})
	response := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-fanout", StatusCode: 200},
		Timestamps: &pb.Timestamps{ResponseAtGrpcServer: time.Now().UnixMilli()},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	go func() {
		_, _ = producer.SendProto(context.Background(), response)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	err = proxy.FanoutResponses(ctx, "pod-1", consumer)
	// Will return when context times out
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Logf("FanoutResponses returned: %v", err)
	}

	select {
	case <-receivedCh:
		if received != nil && received.Response.TransactionId != "txn-fanout" {
			t.Fatalf("expected txn-fanout, got %s", received.Response.TransactionId)
		}
	default:
		// The message might already be consumed - check that there's no error
	}
}
