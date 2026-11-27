// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	pb "github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// BenchmarkProcessHTTPInput: the relay's work per response chunk an HTTP
// agent sends on ConnectHTTP once it is decoded (processHTTPInput: the
// inventory touch, the routing fields, the publish to the proxy). allocs/op
// is per chunk.
func BenchmarkProcessHTTPInput(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := NewDispatcher(ctx, fake.TestLogger(), nil, &fake.TestProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second,
		DispatcherConfig{HeartbeatRecvThreshold: 5 * time.Second, RecvBufferSize: 10, SyncMapClearTimeout: time.Second})
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	stream := NewHTTPStream("agent-1", nil, ctx, make(chan proto.Message, 1), ticker)
	payload := make([]byte, 1<<20)
	in := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{TransactionId: "0123456789abcdef", StatusCode: 200, Payload: payload,
			Headers: map[string]*pb.HeaderValueList{"X-Chunk-Index": {HeaderValues: []string{"3"}}, "X-Chunked": {HeaderValues: []string{"true"}}}},
		Timestamps: &pb.Timestamps{ResponseAtAgent: 7},
	}
	b.ReportAllocs()
	for b.Loop() {
		in.Routing = nil
		if err := d.processHTTPInput(ctx, stream, in, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}
