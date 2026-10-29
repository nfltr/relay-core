// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"maps"
	"net"
	"testing"

	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/client/rpcclient"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// wireLabels holds a value with ',' and '=' (what k=v pairs cannot carry).
var wireLabels = map[string]string{"pool": "web", "zone": "b1", "nfltr.machine.resources": "cpu:16,mem:64GB"}

func TestConnectHTTPAgentWireMetadata(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	const id = "labels-http"
	addr := serveDispatcherAs(t, d, id)
	ctx := t.Context()
	a := agent.New(agent.Config{ServerAddr: addr, AgentID: id, Retries: 1, Logger: slog.New(slog.DiscardHandler)}, agent.WithLabels(wireLabels))
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	info := agentInfo(t, d.GetAllAgentInfo, id)
	if !maps.Equal(info.Labels, wireLabels) {
		t.Errorf("labels = %v, want %v", info.Labels, wireLabels)
	}
	if want := agentwire.Self(); info.Client != want || want.Name == "" {
		t.Errorf("client = %+v, want %+v", info.Client, want)
	}
}

func TestAgentTunnelAgentWireMetadata(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	const id = "labels-a2a"
	addr := serveDispatcherAs(t, d, id)
	ag, err := rpcclient.Dial(context.Background(), addr, rpcclient.WithAgentID(id), rpcclient.WithAPIKey("k"), rpcclient.WithLabels(wireLabels))
	if err != nil {
		t.Fatal(err)
	}
	defer ag.Close() //nolint:errcheck
	info := agentInfo(t, d.GetAllUpstreamAgentInfo, id)
	if !maps.Equal(info.Labels, wireLabels) {
		t.Errorf("labels = %v, want %v", info.Labels, wireLabels)
	}
	if want := agentwire.Self(); info.Client != want || want.Name == "" {
		t.Errorf("client = %+v, want %+v", info.Client, want)
	}
}

// identityStream asserts agentID as the stream's authenticated principal,
// as the API-key interceptor does in production.
type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s identityStream) Context() context.Context { return s.ctx }

// serveDispatcherAs serves d on a loopback listener, every stream
// authenticated as agentID.
func serveDispatcherAs(t *testing.T, d *Dispatcher, agentID string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// WaitForHandlers: Stop returns once the agent's stream handler did,
	// which reads viper (the next test sets it).
	srv := grpc.NewServer(grpcfactory.ProtocolOptions(0)[1], grpc.WaitForHandlers(true), grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		md = md.Copy()
		md.Set("x-apikey-agentID", agentID)
		return h(srv, identityStream{ss, testPrincipal(metadata.NewIncomingContext(ss.Context(), md))})
	}))
	pb.RegisterDispatcherServer(srv, d)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

// agentInfo is agentID's inventory entry: Connect and Dial return once
// the relay registered the agent, so list has it.
func agentInfo(t *testing.T, list func() []AgentInfo, agentID string) AgentInfo {
	t.Helper()
	for _, info := range list() {
		if info.AgentID == agentID {
			return info
		}
	}
	t.Fatalf("%s not registered when its connect returned", agentID)
	return AgentInfo{}
}
