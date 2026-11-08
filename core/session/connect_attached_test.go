// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay-core/internal/platform/testcert"
	"github.com/nfltr/relay-core/tlsconfig"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/client/rpcclient"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const attachedRelayID = "RELAY-B"

// TestConnectNamingThisRelayClaimsNothing: an agent holding a link to this
// relay (through another of its endpoints, here the same one) connects
// again naming this relay's id among the relays it holds
// (agentwire.AgentRelaysKey). The connect is refused ALREADY_ATTACHED
// before it claims anything: the agent's directory record is unchanged
// and its live session is not superseded (spec/Failover.tla Accept; the
// claim-before-dedupe mutant is the bug). One subtest per connect kind.
func TestConnectNamingThisRelayClaimsNothing(t *testing.T) {
	type kind struct {
		name string
		// live starts the agent's session; it returns the inventory key
		// its record is under and a check that it is still the session
		// it started.
		live func(t *testing.T, d *Dispatcher, addr string) (key string, same func() bool)
		// connect opens the named connect of this kind on conn.
		connect func(ctx context.Context, c pb.DispatcherClient) (grpc.ClientStream, error)
		link    bool // the relay serves the agent link
	}
	httpAgent := func(t *testing.T, d *Dispatcher, addr string) (string, func() bool) {
		a := agent.New(agent.Config{ServerAddr: addr, AgentID: attachedAgent, Retries: 1, Logger: slog.New(slog.DiscardHandler)})
		if err := a.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(a.Close)
		s, _ := d.Agents.Load(attachedAgent)
		return directory.GetDispatcherCacheKey(attachedAgent), func() bool {
			cur, ok := d.Agents.Load(attachedAgent)
			return ok && cur == s && s.Context().Err() == nil
		}
	}
	tcpAgent := func(t *testing.T, d *Dispatcher, addr string, tlsOn bool) (string, func() bool) {
		cfg := agent.Config{ServerAddr: addr, AgentID: attachedAgent, TCPTunnel: true, Retries: 1, Logger: slog.New(slog.DiscardHandler)}
		if tlsOn {
			cfg.TLS, cfg.TLSConfig = true, &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h2"}} // #nosec G402 -- test-only self-signed relay cert //nolint:gosec
		}
		a := agent.New(cfg)
		if err := a.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(a.Close)
		waitUntil(t, func() bool {
			return d.TCPDispatcher.agentStream(attachedAgent) != nil && (!tlsOn || d.TCPDispatcher.LinkUp(attachedAgent))
		},
			func() string { return "the agent's TCP stream (and link) did not come up" })
		p := d.TCPDispatcher.agentStream(attachedAgent)
		return directory.GetStreamerCacheKey(attachedAgent), func() bool {
			return d.TCPDispatcher.agentStream(attachedAgent) == p && (!tlsOn || d.TCPDispatcher.LinkUp(attachedAgent))
		}
	}
	connectTCP := func(ctx context.Context, c pb.DispatcherClient) (grpc.ClientStream, error) { return c.ConnectTCP(ctx) }
	for _, k := range []kind{
		{name: "ConnectHTTP", live: httpAgent, connect: func(ctx context.Context, c pb.DispatcherClient) (grpc.ClientStream, error) { return c.ConnectHTTP(ctx) }},
		{name: "AgentTunnel", live: func(t *testing.T, d *Dispatcher, addr string) (string, func() bool) {
			ag, err := rpcclient.Dial(t.Context(), addr, rpcclient.WithAgentID(attachedAgent), rpcclient.WithAPIKey("k"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ag.Close() })
			s, _ := d.Upstreams.Load(attachedAgent)
			return directory.GetStreamerCacheKey(attachedAgent), func() bool {
				cur, ok := d.Upstreams.Load(attachedAgent)
				return ok && cur == s
			}
		}, connect: func(ctx context.Context, c pb.DispatcherClient) (grpc.ClientStream, error) { return c.AgentTunnel(ctx) }},
		{name: "ConnectTCP", live: func(t *testing.T, d *Dispatcher, addr string) (string, func() bool) {
			return tcpAgent(t, d, addr, false)
		}, connect: connectTCP},
		{name: "agent link", link: true, live: func(t *testing.T, d *Dispatcher, addr string) (string, func() bool) {
			return tcpAgent(t, d, addr, true)
		}, connect: connectTCP},
	} {
		t.Run(k.name, func(t *testing.T) {
			d, mc, addr, creds := attachedRelay(t, k.link)
			key, same := k.live(t, d, addr)
			before, _ := mc.Get(t.Context(), key)
			if before == nil || before.Seq == 0 {
				t.Fatalf("the live session wrote no record: %+v", before)
			}

			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close() //nolint:errcheck
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, append(agentwire.Self().HelloPairs(), agentwire.AgentRelaysPairs("RELAY-A", attachedRelayID)...)...)
			s, err := k.connect(ctx, pb.NewDispatcherClient(conn))
			if err == nil {
				err = s.RecvMsg(new(pb.TCPData)) // the refusal ends the call before any message
			}
			if st := status.Convert(err); st.Code() != codes.AlreadyExists || attachedReason(st) != relayv1.ErrorReason_ERROR_REASON_ALREADY_ATTACHED.String() {
				t.Fatalf("named connect: %v, want AlreadyExists with ALREADY_ATTACHED", err)
			}
			after, _ := mc.Get(t.Context(), key)
			if after == nil || after.Seq != before.Seq || after.Since != before.Since || after.PodIP != before.PodIP {
				t.Errorf("the refused connect claimed the record: %+v, was %+v", after, before)
			}
			if !same() {
				t.Error("the refused connect superseded the agent's live session")
			}
		})
	}
}

const attachedAgent = "attached-agent"

// attachedRelay serves a dispatcher with relay id attachedRelayID, its
// inventories over one in-process directory (mc), every stream
// authenticated as attachedAgent; with link, over TLS with the data plane
// (the agent link) and a TCP dispatcher.
func attachedRelay(t *testing.T, link bool) (*Dispatcher, *directory.MemoryCache, string, credentials.TransportCredentials) {
	t.Helper()
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	mc := directory.NewMemoryCache()
	d.IPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	d.StreamerIPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	d.RelayID = attachedRelayID
	d.TCPDispatcher = NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	creds := insecure.NewCredentials()
	var lis net.Listener = ln
	if link {
		cert := testcert.Generate(t)
		lis = d.TCPDispatcher.DataPlaneListener(tlsconfig.WrapGRPCListener(ln, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}}))
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h2"}}) // #nosec G402 -- test-only self-signed relay cert //nolint:gosec
	}
	srv := grpc.NewServer(grpcfactory.ProtocolOptions(0)[1], grpc.WaitForHandlers(true), grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		md = md.Copy()
		md.Set("x-apikey-agentID", attachedAgent)
		return h(srv, identityStream{ss, testPrincipal(metadata.NewIncomingContext(ss.Context(), md))})
	}))
	pb.RegisterDispatcherServer(srv, d)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return d, mc, ln.Addr().String(), creds
}

func attachedReason(st *status.Status) string {
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			return info.GetReason()
		}
	}
	return ""
}
