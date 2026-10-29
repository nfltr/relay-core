// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// registerEchoAgent registers a mock echo agent in the dispatcher and starts
// the receive goroutine that processes responses through the TxnRegistry.
func registerEchoAgent(ctx context.Context, d *Dispatcher, agentID string) {
	es := newEchoStream(ctx, agentID)
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour)
	grpcStream := NewGRPCStream(agentID, es, ctx, heartbeats, ticker)
	d.Upstreams.Store(agentID, grpcStream)
	d.RegisterAgent(agentID, "", "", "")
	go func() { _ = d.receiveFromGRPCAgent(ctx, grpcStream) }()
}

type channelStreamer struct {
	sendCh chan *pb.TunnelFrame
	ctx    context.Context
}

type relayOnlyPodDialer struct {
	client a2aClient
	ctx    context.Context
}

type forbiddenPodDialer struct {
	called chan<- struct{}
}

func (d *forbiddenPodDialer) DialRelay(context.Context, string, *slog.Logger) (*PodRelayStream, error) {
	select {
	case d.called <- struct{}{}:
	default:
	}
	return nil, errors.New("remote dial should not be called")
}

// failingPodDialer simulates a genuine cross-pod dial failure (e.g. the
// replica link could not reach a dead peer pod),
// as opposed to codes.NotFound ("no pod dialer configured" / "not in
// multi-pod mode").
type failingPodDialer struct {
	err error
}

func (d *failingPodDialer) DialRelay(context.Context, string, *slog.Logger) (*PodRelayStream, error) {
	return nil, d.err
}

func (d *relayOnlyPodDialer) DialRelay(_ context.Context, _ string, _ *slog.Logger) (*PodRelayStream, error) {
	stream, err := d.client.RelayTunnel(d.ctx)
	if err != nil {
		return nil, err
	}
	return &PodRelayStream{
		stream:      stream,
		targetPodIP: "bufconn-remote",
		cleanup:     func() {},
	}, nil
}

func (c *channelStreamer) Send(msg *pb.TunnelFrame) error {
	c.sendCh <- msg
	return nil
}

func (c *channelStreamer) Recv() (*pb.TunnelFrame, error) {
	return nil, errors.New("not implemented")
}

func (c *channelStreamer) Context() context.Context {
	return c.ctx
}

func newTestGRPCStream(agentID string, streamer Streamer, ctx context.Context) *GRPCStream {
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Minute)
	return NewGRPCStream(agentID, streamer, ctx, heartbeats, ticker)
}

// newA2ABufConnEnv creates a bufconn-backed gRPC server with a Dispatcher for
// cross-pod A2A integration tests. The echo agent is registered and its
// receive loop is running. Caller must call cleanup().
func newA2ABufConnEnv(t *testing.T) (client a2aClient, dispatcher *Dispatcher, cleanup func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	producer := &fake.TestProducer{}

	cfg := DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		HeartbeatSendDuration:    2 * time.Second,
		HeartbeatEnabled:         false,
		RecvBufferSize:           10,
		SyncMapClearTimeout:      time.Second,
		GRPCPort:                 9090,
		DropAgentOnDownstreamErr: false,
		LoopbackAddr:             "127.0.0.1",
		AdminMessageSet:          true,
	}
	d := NewDispatcher(ctx, fake.TestLogger(), b, producer, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, cfg)
	d.ClusterRelayPeers = true // this env stands in for a peer pod
	d.SingleTenant = true      // its agents are bare ids of one tenant

	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.ChainStreamInterceptor(testPrincipalStream), grpc.ChainUnaryInterceptor(testPrincipalUnary))
	pb.RegisterDispatcherServer(grpcServer, d)
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}

	return a2aClient{pb.NewDispatcherClient(conn), d}, d, func() {
		conn.Close() //nolint:errcheck
		grpcServer.Stop()
		cancel()
	}
}

// a2aClient is the bufconn Dispatcher client plus RelayTunnel, which no
// client can call (relay tunnels are the replica link's): it runs the
// dispatcher's relayTunnel as a replica link peer does.
type a2aClient struct {
	pb.DispatcherClient
	d *Dispatcher
}

func (c a2aClient) RelayTunnel(ctx context.Context) (*relayPipe, error) {
	return openRelayPipe(ctx, c.d), nil
}

// relayPipe is the forwarding end of an in-process relay tunnel.
type relayPipe struct {
	ctx           context.Context
	toSrv, toCli  chan *pb.TunnelFrame
	done          chan struct{}
	err           error
	closeSendOnce sync.Once
}

// relayPipeServer is the end d.relayTunnel serves.
type relayPipeServer struct{ p *relayPipe }

func openRelayPipe(ctx context.Context, d *Dispatcher) *relayPipe {
	p := &relayPipe{ctx: ctx, toSrv: make(chan *pb.TunnelFrame, 16), toCli: make(chan *pb.TunnelFrame, 16), done: make(chan struct{})}
	go func() {
		p.err = d.RelayTunnel(relayPipeServer{p})
		close(p.done)
	}()
	return p
}

func (p *relayPipe) Context() context.Context { return p.ctx }

func (p *relayPipe) Send(m *pb.TunnelFrame) error {
	select {
	case p.toSrv <- m:
		return nil
	case <-p.done:
		return io.EOF
	case <-p.ctx.Done():
		return status.FromContextError(p.ctx.Err()).Err()
	}
}

func (p *relayPipe) Recv() (*pb.TunnelFrame, error) {
	select {
	case m := <-p.toCli:
		return m, nil
	case <-p.ctx.Done():
		return nil, status.FromContextError(p.ctx.Err()).Err()
	case <-p.done:
		select {
		case m := <-p.toCli:
			return m, nil
		default:
		}
		if p.err == nil {
			return nil, io.EOF
		}
		return nil, p.err
	}
}

func (p *relayPipe) CloseSend() error {
	p.closeSendOnce.Do(func() { close(p.toSrv) })
	return nil
}

func (s relayPipeServer) Context() context.Context {
	return context.WithValue(s.p.ctx, PeerLinkCaller{}, true)
}

func (s relayPipeServer) Recv() (*pb.TunnelFrame, error) {
	select {
	case m, ok := <-s.p.toSrv:
		if !ok {
			return nil, io.EOF
		}
		return m, nil
	case <-s.p.ctx.Done():
		return nil, status.FromContextError(s.p.ctx.Err()).Err()
	}
}

func (s relayPipeServer) Send(m *pb.TunnelFrame) error {
	select {
	case s.p.toCli <- m:
		return nil
	case <-s.p.ctx.Done():
		return status.FromContextError(s.p.ctx.Err()).Err()
	}
}

// awaitUpstreamRegistered blocks until the dispatcher has stored agentID's
// upstream. client.AgentTunnel returns once the client-side stream exists,
// before the server handler finishes admission and registration, so an A2A
// route issued right after it can race to "target agent not connected". The
// wait is driven by the dispatcher's registration broadcast (upstreamRegCond),
// not a wall-clock poll; the test deadline bounds it.
func awaitUpstreamRegistered(t *testing.T, d *Dispatcher, agentID string) {
	t.Helper()
	d.UpstreamRegMu.Lock()
	defer d.UpstreamRegMu.Unlock()
	for {
		if _, ok := d.Upstreams.Load(agentID); ok {
			return
		}
		d.UpstreamRegCond.Wait()
	}
}
