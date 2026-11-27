// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package session

import (
	"context"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay-core/platform/config"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/grpccodec"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/experimental"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// countingProducer counts the messages the dispatcher publishes.
type countingProducer struct{ n atomic.Int64 }

func (p *countingProducer) SendBytes(context.Context, []byte) (string, error) { return "", nil }
func (p *countingProducer) SendProto(context.Context, proto.Message) (string, error) {
	p.n.Add(1)
	return "", nil
}
func (p *countingProducer) Close() {}

// BenchmarkConnectHTTPResponseChunks: an HTTP agent streams 1 MiB response
// chunks (chunking.MaxPayload) on ConnectHTTP to the relay over loopback
// gRPC with the production codec and transport options (grpcfactory), the
// relay decoding and publishing each (receiveFromHTTPAgent). cpu-ms/MiB is
// the process's user+system CPU (agent and relay) per MiB relayed.
func BenchmarkConnectHTTPResponseChunks(b *testing.B) {
	viper.Set("cache.type", config.InMemoryCache)
	defer viper.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	published := &countingProducer{}
	msgBroker := broker.NewInMemoryMessageBroker()
	_ = msgBroker.SetTopicAutoCreation("ns", true)
	d := NewDispatcher(ctx, fake.TestLogger(), msgBroker, published, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second,
		DispatcherConfig{ConnectedAgentsThreshold: 10, HeartbeatRecvThreshold: time.Minute, HeartbeatSendDuration: time.Minute, RecvBufferSize: 100, SyncMapClearTimeout: time.Second, GRPCPort: 9090})
	d.SingleTenant = true
	srv := grpc.NewServer(append(grpcfactory.TransportOptions(), grpc.ChainStreamInterceptor(testPrincipalStream))...)
	pb.RegisterDispatcherServer(srv, d)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Stop()
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(grpccodec.NewLargeMessage())), experimental.WithBufferPool(grpccodec.Pool),
		grpc.WithInitialWindowSize(16<<20), grpc.WithInitialConnWindowSize(16<<20))
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	stream, err := pb.NewDispatcherClient(conn).ConnectHTTP(metadata.AppendToOutgoingContext(ctx, "x-apikey-agentID", "bench-http-agent"))
	if err != nil {
		b.Fatal(err)
	}
	chunk := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{TransactionId: "0123456789abcdef", StatusCode: 200, Payload: make([]byte, 1<<20),
			Headers: map[string]*pb.HeaderValueList{"X-Chunk-Index": {HeaderValues: []string{"3"}}, "X-Chunked": {HeaderValues: []string{"true"}}}},
		Timestamps: &pb.Timestamps{ResponseAtAgent: 7},
	}
	send := func(n int64) {
		want := published.n.Load() + n
		for range n {
			if err := stream.Send(chunk); err != nil {
				b.Fatal(err)
			}
		}
		for deadline := time.Now().Add(time.Minute); published.n.Load() < want; time.Sleep(50 * time.Microsecond) {
			if time.Now().After(deadline) { // only against a hang
				b.Fatalf("the relay published %d of %d chunks", published.n.Load(), want)
			}
		}
	}
	send(16) // the session is up and warm
	b.SetBytes(1 << 20)
	b.ReportAllocs()
	cpu0 := processCPU()
	b.ResetTimer()
	send(int64(b.N))
	b.StopTimer()
	b.ReportMetric(float64((processCPU()-cpu0).Milliseconds())/float64(b.N), "cpu-ms/MiB")
}

func processCPU() time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
