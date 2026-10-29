// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestAgentStreamsStateTheHeartbeat(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		d.Config.HeartbeatEnabled = enabled
		client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "hb-agent"))
		want := ""
		if enabled {
			want = "2000" // newTestDispatcher's 2s
		}

		httpStream, err := client.ConnectHTTP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		a2aStream, err := client.AgentTunnel(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for name, header := range map[string]func() (metadata.MD, error){"ConnectHTTP": httpStream.Header, "AgentTunnel": a2aStream.Header} {
			md, err := header() // sent on accept, before any frame
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got := firstOf(md.Get(agentwire.HeartbeatKey)); got != want {
				t.Errorf("heartbeats on=%v, %s states %q, want %q", enabled, name, got, want)
			}
		}
	}
}

// An agent's liveness asks for its heartbeat; the relay keeps it within
// [1s, a third of its receive threshold] and states what it keeps.
func TestAgentStreamsKeepTheHeartbeatAsked(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.HeartbeatEnabled = true
	d.Config.HeartbeatRecvThreshold = 45 * time.Second
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "hb-ask"))
	for ask, want := range map[string]string{"1500": "1500", "200": "1000", "60000": "15000", "nonsense": "2000"} {
		askCtx := metadata.AppendToOutgoingContext(ctx, agentwire.HeartbeatAskKey, ask)
		httpStream, err := client.ConnectHTTP(askCtx)
		if err != nil {
			t.Fatal(err)
		}
		a2aStream, err := client.AgentTunnel(askCtx)
		if err != nil {
			t.Fatal(err)
		}
		for name, header := range map[string]func() (metadata.MD, error){"ConnectHTTP": httpStream.Header, "AgentTunnel": a2aStream.Header} {
			md, err := header()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if got := firstOf(md.Get(agentwire.HeartbeatKey)); got != want {
				t.Errorf("asked %s: %s states %q, want %q", ask, name, got, want)
			}
		}
		_ = httpStream.CloseSend()
		_ = a2aStream.CloseSend()
	}
}

func firstOf(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// A fast heartbeat refreshes the agent's last-seen record no more than
// every heartbeatBookkeepingEvery.
func TestHeartbeatBookkeepingKeepsItsCadence(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.Config.HeartbeatSendDuration = 10 * time.Millisecond
	var bookkept atomic.Int32
	d.OnAgentHeartbeat = func(string) { bookkept.Add(1) }
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "hb-bookkeeping"))
	for name, open := range map[string]func() (grpc.ClientStream, error){
		"ConnectHTTP": func() (grpc.ClientStream, error) { return client.ConnectHTTP(ctx) },
		"AgentTunnel": func() (grpc.ClientStream, error) { return client.AgentTunnel(ctx) },
	} {
		bookkept.Store(0)
		stream, err := open()
		if err != nil {
			t.Fatal(err)
		}
		// Ten heartbeats, not 300 ms of them: a loaded runner sent five in
		// 300 ms. Ten take far less than
		// heartbeatBookkeepingEvery (15 s), so one refresh is all.
		heartbeats := 0
		for deadline := time.Now().Add(10 * time.Second); heartbeats < 10 && time.Now().Before(deadline); heartbeats++ {
			var msg any = new(pb.HTTPProxyRequest)
			if name == "AgentTunnel" {
				msg = new(pb.TunnelFrame)
			}
			if err := stream.RecvMsg(msg); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		}
		_ = stream.CloseSend()
		if heartbeats < 10 || bookkept.Load() != 1 {
			t.Errorf("%s: %d heartbeats, %d last-seen refreshes; want many heartbeats and one refresh", name, heartbeats, bookkept.Load())
		}
	}
}

func dialDispatcher(t *testing.T, addr string) (pb.DispatcherClient, context.Context) {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewDispatcherClient(conn), metadata.NewOutgoingContext(t.Context(), metadata.Pairs(agentwire.Self().HelloPairs()...))
}

// A relay that offers the agent link states, on ConnectHTTP, that it takes
// an agent's liveness from the link while it is up
// (agentwire.LinkLivenessKey); one without the agent link (or without
// heartbeats) does not, and its agents answer every heartbeat.
func TestConnectHTTPStatesLinkLiveness(t *testing.T) {
	for _, withLink := range []bool{true, false} {
		d, cancel := newTestDispatcher(t)
		defer cancel()
		d.Config.HeartbeatEnabled = true
		if withLink {
			d.TCPDispatcher = newTestAgentLink(t).d
			d.TCPDispatcher.Link = newAgentLink(d.TCPDispatcher, nil)
		}
		client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "ll-agent"))
		s, err := client.ConnectHTTP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		md, err := s.Header()
		if err != nil {
			t.Fatal(err)
		}
		if got := firstOf(md.Get(agentwire.LinkLivenessKey)) == "1"; got != withLink {
			t.Errorf("agent link offered=%v: states link liveness %v", withLink, got)
		}
	}
}
