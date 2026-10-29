// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/util"
	pbpkg "github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const benchBufSize = 1024 * 1024

// echoStream is a mock Streamer that echoes sent messages back via Recv.
// This simulates an agent that instantly replies to every request.
type echoStream struct {
	ctx     context.Context
	ch      chan *pbpkg.TunnelFrame // messages from dispatcher → agent → back
	agentID string
}

func newEchoStream(ctx context.Context, agentID string) *echoStream {
	return &echoStream{ctx: ctx, ch: make(chan *pbpkg.TunnelFrame, 100), agentID: agentID}
}

func (e *echoStream) Send(msg *pbpkg.TunnelFrame) error {
	// Build echo response matching what a real agent would do
	resp := util.BytesToTunnelFrame(
		msg.GetPayload(),
		msg.GetMessageType(),
		e.agentID,
		msg.GetEom(),
	)
	resp.TransactionId = msg.GetTransactionId()
	resp.Timestamps = &pbpkg.Timestamps{
		RequestAtAgent:  time.Now().UnixMilli(),
		ResponseAtAgent: time.Now().UnixMilli(),
	}
	select {
	case e.ch <- resp:
		return nil
	case <-e.ctx.Done():
		return e.ctx.Err()
	}
}

func (e *echoStream) Recv() (*pbpkg.TunnelFrame, error) {
	select {
	case msg := <-e.ch:
		return msg, nil
	case <-e.ctx.Done():
		return nil, e.ctx.Err()
	}
}

func (e *echoStream) Context() context.Context { return e.ctx }

// benchEnv holds the shared test infrastructure for dispatch benchmarks.
type benchEnv struct {
	listener   *bufconn.Listener
	grpcServer *grpc.Server
	dispatcher *Dispatcher
	cancel     context.CancelFunc
}

// newBenchEnv sets up an in-process gRPC server backed by bufconn with a
// Dispatcher using in-memory broker and no-op inventory. The caller must
// call cleanup() when done.
func newBenchEnv(b *testing.B) *benchEnv {
	b.Helper()

	viper.Set("cache.type", config.InMemoryCache)

	ctx, cancel := context.WithCancel(context.Background())

	msgBroker := broker.NewInMemoryMessageBroker()
	_ = msgBroker.SetTopicAutoCreation("ns", true)

	podIDTopic := "bench"
	podIDTopicProducer, _ := msgBroker.CreateProducer(broker.ProducerOptions{
		Topic: podIDTopic,
	})

	dispatcher := NewDispatcher(
		ctx, fake.TestLogger(), msgBroker, podIDTopicProducer,
		directory.NewNoOpInventory(), directory.NewNoOpInventory(), 10*time.Second,
		DispatcherConfig{
			ConnectedAgentsThreshold: 10000,
			HeartbeatRecvThreshold:   60 * time.Second,
			HeartbeatSendDuration:    30 * time.Second,
			HeartbeatEnabled:         false,
			RecvBufferSize:           100,
			SyncMapClearTimeout:      time.Second,
			GRPCPort:                 9090,
			DropAgentOnDownstreamErr: false,
		},
	)

	lis := bufconn.Listen(benchBufSize)
	grpcServer := grpc.NewServer(grpc.ChainStreamInterceptor(testPrincipalStream), grpc.ChainUnaryInterceptor(testPrincipalUnary))
	pbpkg.RegisterDispatcherServer(grpcServer, dispatcher)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("bench grpc server exited: %v", err)
		}
	}()

	return &benchEnv{
		listener:   lis,
		grpcServer: grpcServer,
		dispatcher: dispatcher,
		cancel:     cancel,
	}
}

func (env *benchEnv) cleanup() {
	env.grpcServer.Stop()
	env.cancel()
	viper.Reset()
}

// dial connects as an authenticated agent (the bench server has no auth
// interceptor, so the identity the interceptor would set travels as
// metadata).
func (env *benchEnv) dial() (*grpc.ClientConn, error) {
	return grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return env.listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(metadata.AppendToOutgoingContext(ctx, "x-apikey-agentID", "bench-caller"), desc, cc, method, opts...)
		}),
	)
}

// registerMockAgent creates a mock echo agent, stores it in the dispatcher's
// upstreams map, and starts the receiveFromGRPCAgent goroutine that processes
// agent responses through the TxnRegistry. This is the same path used by
// production AgentTunnel connections.
func (env *benchEnv) registerMockAgent(ctx context.Context, agentID string) {
	es := newEchoStream(ctx, agentID)
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour) // never fires

	grpcStream := NewGRPCStream(agentID, es, ctx, heartbeats, ticker)
	env.dispatcher.Upstreams.Store(agentID, grpcStream)
	env.dispatcher.RegisterAgent(agentID, "", "", "")

	// Start the same response-processing loop the AgentTunnel handler runs.
	go func() {
		_ = env.dispatcher.receiveFromGRPCAgent(ctx, grpcStream)
	}()
}

// ─── Benchmarks ──────────────────────────────────────────────────────────────

// BenchmarkRelayTunnelRoundTrip measures the full dispatch latency of a single
// relay tunnel request → agent echo → response cycle, as a replica link peer
// forwards it. This exercises the core hot path: Dispatcher.relayTunnel → relayRequestsToAgent
// → echoStream → receiveFromGRPCAgent → TxnRegistry.Fanout → response.
func BenchmarkRelayTunnelRoundTrip(b *testing.B) {
	env := newBenchEnv(b)
	defer env.cleanup()

	ctx := b.Context()

	agentID := "bench-agent-1"
	env.registerMockAgent(ctx, agentID)

	conn, err := env.dial()
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	client := a2aClient{pbpkg.NewDispatcherClient(conn), env.dispatcher}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stream, err := client.RelayTunnel(ctx)
		if err != nil {
			b.Fatal(err)
		}

		msg := util.BytesToTunnelFrame([]byte("bench-payload"), "test", agentID, true)
		msg.Timestamps = &pbpkg.Timestamps{
			RequestAtHttpServer: time.Now().Unix(),
			RequestAtGrpcServer: time.Now().Unix(),
		}
		if err := stream.Send(msg); err != nil {
			b.Fatal(err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				b.Fatalf("recv error on iteration %d: %v", i, err)
			}
			if resp.GetEom() {
				break
			}
		}
	}
}

// BenchmarkRelayTunnelRoundTripParallel measures dispatch throughput with
// concurrent RelayTunnel callers sharing a pool of agents via bufconn.
func BenchmarkRelayTunnelRoundTripParallel(b *testing.B) {
	env := newBenchEnv(b)
	defer env.cleanup()

	ctx := b.Context()

	const numAgents = 4
	for i := 1; i <= numAgents; i++ {
		env.registerMockAgent(ctx, fmt.Sprintf("bench-agent-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(bpb *testing.PB) {
		conn, err := env.dial()
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		dClient := a2aClient{pbpkg.NewDispatcherClient(conn), env.dispatcher}

		agentIdx := 1
		for bpb.Next() {
			agentID := fmt.Sprintf("bench-agent-%d", agentIdx)
			agentIdx = (agentIdx % numAgents) + 1

			stream, err := dClient.RelayTunnel(ctx)
			if err != nil {
				b.Fatal(err)
			}

			msg := util.BytesToTunnelFrame([]byte("bench-payload"), "test", agentID, true)
			msg.Timestamps = &pbpkg.Timestamps{
				RequestAtHttpServer: time.Now().Unix(),
				RequestAtGrpcServer: time.Now().Unix(),
			}
			if err := stream.Send(msg); err != nil {
				b.Fatal(err)
			}
			for {
				resp, err := stream.Recv()
				if err != nil {
					b.Fatal(err)
				}
				if resp.GetEom() {
					break
				}
			}
		}
	})
}

// BenchmarkDownStreamDispatch measures the downstream dispatch hot path:
// accepting a gRPC RelayTunnel request, routing to a local agent, collecting
// the response, and sending it back. This is the primary PGO profile target
// for the dispatcher package (referenced by Makefile Phase 1a).
func BenchmarkDownStreamDispatch(b *testing.B) {
	env := newBenchEnv(b)
	defer env.cleanup()

	ctx := b.Context()

	agentID := "bench-ds-agent-1"
	env.registerMockAgent(ctx, agentID)

	conn, err := env.dial()
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	client := a2aClient{pbpkg.NewDispatcherClient(conn), env.dispatcher}

	payload := []byte("downstream-bench-payload-64bytes-padded-for-realistic-sizing!!")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stream, err := client.RelayTunnel(ctx)
		if err != nil {
			b.Fatal(err)
		}

		msg := util.BytesToTunnelFrame(payload, "test", agentID, true)
		msg.Timestamps = &pbpkg.Timestamps{
			RequestAtHttpServer: time.Now().Unix(),
			RequestAtGrpcServer: time.Now().Unix(),
		}
		if err := stream.Send(msg); err != nil {
			b.Fatal(err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				b.Fatalf("recv error on iteration %d: %v", i, err)
			}
			if resp.GetEom() {
				break
			}
		}
	}
}

// BenchmarkDownStreamDispatchParallel measures concurrent downstream dispatch
// throughput with multiple callers sharing a pool of agents.
func BenchmarkDownStreamDispatchParallel(b *testing.B) {
	env := newBenchEnv(b)
	defer env.cleanup()

	ctx := b.Context()

	const numAgents = 8
	for i := 1; i <= numAgents; i++ {
		env.registerMockAgent(ctx, fmt.Sprintf("bench-ds-agent-%d", i))
	}

	payload := []byte("downstream-bench-payload-64bytes-padded-for-realistic-sizing!!")

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(bpb *testing.PB) {
		conn, err := env.dial()
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close() //nolint:errcheck
		dClient := a2aClient{pbpkg.NewDispatcherClient(conn), env.dispatcher}

		agentIdx := 1
		for bpb.Next() {
			agentID := fmt.Sprintf("bench-ds-agent-%d", agentIdx)
			agentIdx = (agentIdx % numAgents) + 1

			stream, err := dClient.RelayTunnel(ctx)
			if err != nil {
				b.Fatal(err)
			}

			msg := util.BytesToTunnelFrame(payload, "test", agentID, true)
			msg.Timestamps = &pbpkg.Timestamps{
				RequestAtHttpServer: time.Now().Unix(),
				RequestAtGrpcServer: time.Now().Unix(),
			}
			if err := stream.Send(msg); err != nil {
				b.Fatal(err)
			}
			for {
				resp, err := stream.Recv()
				if err != nil {
					b.Fatal(err)
				}
				if resp.GetEom() {
					break
				}
			}
		}
	})
}

// BenchmarkDownStreamLargePayload measures dispatch latency with a 1KB payload
// to profile serialisation and memory allocation under realistic message sizes.
func BenchmarkDownStreamLargePayload(b *testing.B) {
	env := newBenchEnv(b)
	defer env.cleanup()

	ctx := b.Context()

	agentID := "bench-ds-large-agent"
	env.registerMockAgent(ctx, agentID)

	conn, err := env.dial()
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	client := a2aClient{pbpkg.NewDispatcherClient(conn), env.dispatcher}

	payload := make([]byte, 1024) // 1KB payload
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		stream, err := client.RelayTunnel(ctx)
		if err != nil {
			b.Fatal(err)
		}

		msg := util.BytesToTunnelFrame(payload, "test", agentID, true)
		msg.Timestamps = &pbpkg.Timestamps{
			RequestAtHttpServer: time.Now().Unix(),
			RequestAtGrpcServer: time.Now().Unix(),
		}
		if err := stream.Send(msg); err != nil {
			b.Fatal(err)
		}

		for {
			resp, err := stream.Recv()
			if err != nil {
				b.Fatalf("recv error on iteration %d: %v", i, err)
			}
			if resp.GetEom() {
				break
			}
		}
	}
}
