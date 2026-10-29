// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

type mockStreamer struct {
	sendErr error
	recvMsg *pb.TunnelFrame
	recvErr error
}

func (m *mockStreamer) Context() context.Context { return context.Background() }

func (m *mockStreamer) Send(_ *pb.TunnelFrame) error { return m.sendErr }

func (m *mockStreamer) Recv() (*pb.TunnelFrame, error) { return m.recvMsg, m.recvErr }

func TestStream_Methods(t *testing.T) {
	ctx := context.Background()
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	mock := &mockStreamer{}
	s := NewGRPCStream("agentID-1", mock, ctx, heartbeats, ticker)

	if s.AgentID() != "agentID-1" {
		t.Fatal("unexpected agentID")
	}
	if s.Context() != ctx {
		t.Fatal("unexpected context")
	}
	if s.Heartbeats() != heartbeats {
		t.Fatal("unexpected heartbeats channel")
	}
	if s.HeartbeatsRecvThresholdTicker() != ticker {
		t.Fatal("unexpected ticker")
	}

	// Test Send
	if err := s.Send(&pb.TunnelFrame{}); err != nil {
		t.Fatal(err)
	}

	// Test Recv
	mock.recvMsg = &pb.TunnelFrame{}
	msg, err := s.Recv()
	if err != nil || msg == nil {
		t.Fatal("unexpected error or nil msg")
	}
}

func TestAgentStream_Methods(t *testing.T) {
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	mock := &mockHTTPStreamer{}
	s := NewHTTPStream("agentID-2", mock, nil, heartbeats, ticker)

	if s.AgentID() != "agentID-2" {
		t.Fatal("unexpected agentID")
	}
	if s.Heartbeats() != heartbeats {
		t.Fatal("unexpected heartbeats channel")
	}
	if s.HeartbeatsRecvThresholdTicker() != ticker {
		t.Fatal("unexpected ticker")
	}

	// Test Send
	if err := s.Send(&pb.HTTPProxyRequest{}); err != nil {
		t.Fatal(err)
	}

	// Test Recv
	mock.recvMsg = &pb.HTTPProxyResponse{}
	msg, err := s.Recv()
	if err != nil || msg == nil {
		t.Fatal("unexpected error or nil msg")
	}
}

func TestStream_OutboundDataSentWithinTracksOnlyEmptyHeartbeats(t *testing.T) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	grpcStream := NewGRPCStream("agentID-grpc", &mockStreamer{}, context.Background(), make(chan proto.Message, 1), ticker)
	if err := grpcStream.Send(&pb.TunnelFrame{}); err != nil {
		t.Fatal(err)
	}
	if grpcStream.OutboundDataSentWithin(time.Second) {
		t.Fatal("empty gRPC heartbeat was tracked as outbound data")
	}
	if err := grpcStream.Send(&pb.TunnelFrame{MessageType: "control"}); err != nil {
		t.Fatal(err)
	}
	if !grpcStream.OutboundDataSentWithin(time.Second) {
		t.Fatal("non-empty gRPC frame without transaction ID was not tracked as outbound data")
	}

	httpStream := NewHTTPStream("agentID-http", &mockHTTPStreamer{}, nil, make(chan proto.Message, 1), ticker)
	if err := httpStream.Send(&pb.HTTPProxyRequest{}); err != nil {
		t.Fatal(err)
	}
	if httpStream.OutboundDataSentWithin(time.Second) {
		t.Fatal("empty HTTP heartbeat was tracked as outbound data")
	}
	if err := httpStream.Send(&pb.HTTPProxyRequest{Request: &pb.HTTPRequest{Method: "GET"}}); err != nil {
		t.Fatal(err)
	}
	if !httpStream.OutboundDataSentWithin(time.Second) {
		t.Fatal("non-empty HTTP request without transaction ID was not tracked as outbound data")
	}
}

type mockHTTPStreamer struct {
	sendErr error
	recvMsg *pb.HTTPProxyResponse
	recvErr error
}

func (m *mockHTTPStreamer) Send(_ *pb.HTTPProxyRequest) error { return m.sendErr }

func (m *mockHTTPStreamer) Recv() (*pb.HTTPProxyResponse, error) { return m.recvMsg, m.recvErr }
