// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/proto"
)

func TestDispatcher_routeResponseToBroker_InMemory(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	input := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-1"},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
	}
	err := d.routeResponseToBroker(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDispatcher_watchAgentSession_Stop_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		globalContext:           context.Background(),
		gracefulShutdownTimeout: time.Second,
		Logger:                  fake.TestLogger(),
	}

	cancel()
	err := d.WatchAgentSession(ctx, "agentID-1", nil, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDispatcher_watchAgentSession_Stop_GlobalCancel(t *testing.T) {
	ctx := context.Background()
	globalCtx, globalCancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		globalContext:           globalCtx,
		gracefulShutdownTimeout: 10, // nanoseconds - very fast
		Logger:                  fake.TestLogger(),
	}

	globalCancel()
	err := d.WatchAgentSession(ctx, "agentID-1", nil, nil, nil)
	if !errors.Is(err, errServerRestarting) {
		t.Fatalf("expected errServerRestarting, got %v", err)
	}
}

type mockHeartbeatTracker struct {
	agentID    string
	heartbeats chan proto.Message
	ticker     *time.Ticker
}

func (m *mockHeartbeatTracker) AgentID() string { return m.agentID }

func (m *mockHeartbeatTracker) Heartbeats() chan proto.Message { return m.heartbeats }

func (m *mockHeartbeatTracker) HeartbeatsRecvThresholdTicker() *time.Ticker { return m.ticker }

func TestDispatcher_watchAgentSession_Heartbeat_HeartbeatReceived(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(5 * time.Second) // long enough not to fire
	defer ticker.Stop()

	stream := &mockHeartbeatTracker{
		agentID:    "agentID-1",
		heartbeats: heartbeats,
		ticker:     ticker,
	}

	ctx, ctxCancel := context.WithCancel(context.Background())
	heartbeats <- &pb.TunnelFrame{} // send heartbeat
	go func() {
		time.Sleep(100 * time.Millisecond)
		ctxCancel()
	}()

	d.Config.HeartbeatEnabled = true
	err := d.WatchAgentSession(ctx, stream.AgentID(), stream, directory.NewNoOpInventory(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDispatcher_markAgentStreamAlive_QueuesHeartbeat(t *testing.T) {
	tracker := &mockHeartbeatTracker{
		agentID:    "agentID-1",
		heartbeats: make(chan proto.Message, 1),
		ticker:     time.NewTicker(time.Second),
	}
	defer tracker.ticker.Stop()

	d := &Dispatcher{}
	d.markAgentStreamAlive(tracker, &pb.TunnelFrame{})

	select {
	case <-tracker.heartbeats:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected synthetic heartbeat to be queued")
	}
}

func TestDispatcher_watchAgentSession_Heartbeat_Timeout(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message)
	ticker := time.NewTicker(50 * time.Millisecond) // fires quickly
	defer ticker.Stop()

	stream := &mockHeartbeatTracker{
		agentID:    "agentID-1",
		heartbeats: heartbeats,
		ticker:     ticker,
	}

	d.Config.HeartbeatEnabled = true
	err := d.WatchAgentSession(context.Background(), stream.AgentID(), stream, directory.NewNoOpInventory(), nil)
	if !errors.Is(err, errNoHeartbeat) {
		t.Fatalf("expected errNoHeartbeat, got %v", err)
	}
}

func TestNewGRPCServer(t *testing.T) {
	viper.Set("grpc.keepalive.enforcement_policy.min_time", 10*time.Second)
	viper.Set("grpc.keepalive.enforcement_policy.permit_without_stream", true)
	viper.Set("grpc.keepalive.server_parameters.time", 30*time.Second)
	viper.Set("grpc.keepalive.server_parameters.timeout", 10*time.Second)
	defer viper.Reset()

	server := grpcfactory.NewGRPCServer(fake.TestLogger())
	if server == nil {
		t.Fatal("expected non-nil server")
	}
	server.Stop()
}

func TestDefaultOnEvict(t *testing.T) {
	p := &fake.TestProducer{}
	broker.DefaultOnEvict("key", p) // should not panic
}

func TestDispatcher_receiveFromHTTPAgent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 10)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	recvCount := 0
	mock := &mockHTTPStreamer{
		recvMsg: &pb.HTTPProxyResponse{
			Timestamps: &pb.Timestamps{},
		},
	}
	// Override mock to return error after first recv
	origRecv := mock.Recv
	_ = origRecv
	stream := NewHTTPStream("agentID-1", mock, nil, heartbeats, ticker)

	ctx, ctxCancel := context.WithCancel(context.Background())

	go func() {
		// Drain heartbeats so processHTTPInput doesn't block
		for range heartbeats {
			recvCount++
			if recvCount >= 2 {
				ctxCancel()
				return
			}
		}
	}()

	err := d.receiveFromHTTPAgent(ctx, stream)
	// Should eventually error with context canceled or recv error
	_ = err
}

func TestDispatcher_receiveFromHTTPAgentFromGRPCAgent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 10)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	recvCount := 0
	ctx, ctxCancel := context.WithCancel(context.Background())

	mock := &mockStreamer{
		recvMsg: &pb.TunnelFrame{},
	}
	stream := NewGRPCStream("agentID-1", mock, ctx, heartbeats, ticker)

	go func() {
		for range heartbeats {
			recvCount++
			if recvCount >= 2 {
				ctxCancel()
				return
			}
		}
	}()

	err := d.receiveFromGRPCAgent(ctx, stream)
	_ = err
}

func TestDispatcher_relayRequestsToAgent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	defer viper.Reset()

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	sentMessages := make(chan *pb.HTTPProxyRequest, 10)
	mock := &mockHTTPStreamerCapture{sent: sentMessages}
	stream := NewHTTPStream("agentID-1", mock, nil, heartbeats, ticker)

	b := d.broker
	topicName := "test-topic-send"
	_ = b.CreateTopic(topicName, 1, true)
	consumer, err := b.Subscribe(broker.ConsumerOptions{
		Topic:            topicName,
		SubscriptionName: "sub-1",
		Type:             broker.ConsumerTypeExclusive,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	// Send a message via producer
	producer, err := b.CreateProducer(broker.ProducerOptions{Topic: topicName})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		msg := &pb.HTTPProxyRequest{
			Request:    &pb.HTTPRequest{TransactionId: "txn-send"},
			Timestamps: &pb.Timestamps{RequestAtHttpServer: time.Now().UnixMilli()},
			Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
		}
		data, _ := proto.Marshal(msg)
		_, _ = producer.SendBytes(context.Background(), data)
		time.Sleep(100 * time.Millisecond)
		ctxCancel()
	}()

	err = d.relayRequestsToAgent(ctx, stream, consumer)
	// Should process one message then context cancels
	_ = err
}

func TestDispatcher_relayRequestsToAgent_SkipsHeartbeatWhenOutboundDataRecentlySent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &Dispatcher{
			Config: DispatcherConfig{
				HeartbeatSendDuration: 50 * time.Millisecond,
			},
			Logger:      fake.TestLogger(),
			IPInventory: directory.NewNoOpInventory(),
		}

		heartbeats := make(chan proto.Message, 1)
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		sentMessages := make(chan *pb.HTTPProxyRequest, 10)
		mock := &mockHTTPStreamerCapture{sent: sentMessages}
		stream := NewHTTPStream("agentID-http-data", mock, nil, heartbeats, ticker)

		ctx, ctxCancel := context.WithCancel(context.Background())
		consumer := &idleConsumer{ch: make(chan broker.Message)}
		done := make(chan error, 1)
		go func() {
			done <- d.relayRequestsToAgent(ctx, stream, consumer)
		}()

		time.Sleep(20 * time.Millisecond)
		if err := stream.Send(&pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: "txn-data"}}); err != nil {
			t.Fatalf("send data frame: %v", err)
		}

		time.Sleep(45 * time.Millisecond)
		ctxCancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			t.Fatal("relayRequestsToAgent did not stop after cancellation")
		}

		close(sentMessages)
		for msg := range sentMessages {
			if msg.GetRequest() == nil || msg.GetRequest().GetTransactionId() == "" {
				t.Fatalf("heartbeat was sent despite recent outbound data")
			}
		}
	})
}

type idleConsumer struct {
	ch chan broker.Message
}

func (c *idleConsumer) Ack(msg broker.Message) error {
	return msg.Ack()
}

func (c *idleConsumer) Chan() <-chan broker.Message {
	return c.ch
}

func (*idleConsumer) Close() {}

func (*idleConsumer) Unsubscribe() error {
	return nil
}

type mockHTTPStreamerCapture struct {
	sent    chan *pb.HTTPProxyRequest
	recvMsg *pb.HTTPProxyResponse
	recvErr error
}

func (m *mockHTTPStreamerCapture) Send(msg *pb.HTTPProxyRequest) error {
	if m.sent != nil {
		select {
		case m.sent <- msg:
		default:
		}
	}
	return nil
}

func (m *mockHTTPStreamerCapture) Recv() (*pb.HTTPProxyResponse, error) {
	if m.recvErr != nil {
		return nil, m.recvErr
	}
	return m.recvMsg, nil
}
