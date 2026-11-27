// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc/metadata"
)

// linkConns is how many agent-link connections the harness relay holds.
func linkConns(h *tunnelHarness) int {
	h.D.Link.Mu.Lock()
	defer h.D.Link.Mu.Unlock()
	n := 0
	for _, m := range h.D.Link.Conns {
		n += len(m)
	}
	return n
}

// TestAgentLinkUpForThePrimarysLife: on each transport an agent's link is
// up as soon as its primary registers, idle or not (its liveness is the
// agent's: LinkUp); sessions ride it, and it stays up while the agent idles
// between them, past what once retired it (auxIdle).
func TestAgentLinkUpForThePrimarysLife(t *testing.T) {
	harnessAuxIdle = 200 * time.Millisecond
	t.Cleanup(func() { harnessAuxIdle = 0 })
	for _, tc := range linkTransportAgents {
		t.Run(tc.transport, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, tc.ag)
			waitUntil(t, func() bool { return linkConns(h) == 1 },
				func() string {
					return fmt.Sprintf("idle agent holds %d agent-link connections, want its link up", linkConns(h))
				})
			if !h.D.LinkUp(harnessAgentID) {
				t.Fatal("LinkUp false with the link connected")
			}
			for round := range 2 {
				c := h.Dial(t)
				if _, err := c.Write(targetHeader('e', 0)); err != nil {
					t.Fatal(err)
				}
				_, _ = c.Write([]byte("x"))
				b := make([]byte, 1)
				if _, err := io.ReadFull(c, b); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
				if got := linkTransports(h); len(got) != 1 || got[0] != tc.transport {
					t.Fatalf("round %d: agent link on %v, want %s", round, got, tc.transport)
				}
				_ = c.Close()
				time.Sleep(3 * harnessAuxIdle) // idle past what once retired it
				if n := linkConns(h); n != 1 {
					t.Fatalf("round %d: %d agent-link connections after idling, want the link kept", round, n)
				}
			}
			if h.Streams() != 1 {
				t.Errorf("%d frame streams, want the primary only", h.Streams())
			}
		})
	}
}

// TestAgentLinkIdleFleet: 20 idle agents on one relay each hold their
// agent link (one connection each), which a session to one of them rides.
func TestAgentLinkIdleFleet(t *testing.T) {
	const agents = 20
	target := startTarget(t)
	h := startHarness(t, 0, relayCurrent, agentCurrent, func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
		var wg sync.WaitGroup
		for i := range agents {
			wg.Go(func() {
				id := fmt.Sprintf("fleet-%d", i)
				if i == 0 {
					id = harnessAgentID // the one sessions go to
				}
				sctx := metadata.AppendToOutgoingContext(ctx, harnessAgentHeader, id)
				_ = agent.RunTCPTunnel(ctx, client, dial, sctx, "", logger)
			})
		}
		wg.Wait()
	}, func(ctx context.Context, d *TCPDispatcher, c net.Conn) {
		d.HandleConn(ctx, c, TCPEndpoint{AgentID: harnessAgentID, TargetAddr: target})
	})
	waitUntil(t, func() bool { return linkConns(h) == agents },
		func() string {
			return fmt.Sprintf("%d idle agents hold %d agent-link connections, want one each", agents, linkConns(h))
		})
	if err := echo(h, t, []byte("one agent")); err != nil {
		t.Fatal(err)
	}
	if n := linkConns(h); n != agents {
		t.Fatalf("%d agent-link connections after one agent's session, want %d", n, agents)
	}
}

// TestAgentLinkDialUnanswered: an agent that asked for the link but does
// not dial it: concurrent sessions send it one request and take the frame
// streams within agentLinkDialWait; later ones wait no more.
func TestAgentLinkDialUnanswered(t *testing.T) {
	l := newTestAgentLink(t)
	p := NewTCPAgentStream()
	p.agent, p.token = "agent", "silent-token"
	l.d.bound[p.token] = p
	p.link.asked = dataplane.AgentLinkTCP
	start := time.Now()
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, _, ok, err := l.open(context.Background(), p, &pb.TCPData{TargetAddr: "x:1"}); ok || err != nil {
				t.Errorf("open: ok %v err %v, want the frame streams", ok, err)
			}
		})
	}
	wg.Wait()
	if d := time.Since(start); d < agentLinkDialWait || d > agentLinkDialWait+500*time.Millisecond {
		t.Errorf("sessions took the frame streams after %s, want %s", d, agentLinkDialWait)
	}
	if n := len(p.SendCh); n != 1 {
		t.Fatalf("%d frames to the agent, want one dial request", n)
	}
	if f := <-p.SendCh; f.GetSessionId() != dataplane.AgentLinkSID || string(f.GetData()) != dataplane.AgentLinkDial {
		t.Fatalf("frame %v, want the dial request", f)
	}
	start = time.Now()
	if _, _, ok, _ := l.open(context.Background(), p, &pb.TCPData{TargetAddr: "x:1"}); ok {
		t.Fatal("open on a link that never came up")
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Errorf("a later session waited %s for the link", d)
	}
	if n := len(p.SendCh); n != 0 {
		t.Errorf("%d more requests to an agent that did not answer", n)
	}
}

// TestAgentLinkEndsWithItsPort: closing the gRPC port's listener ends the
// agent link's TCP Node (its Serve, waiting in Accept) too.
func TestAgentLinkEndsWithItsPort(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = d.DataPlaneListener(inner)
	_ = inner.Close()
	select {
	case <-d.Link.ln.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the agent link's listener is still open after its port closed")
	}
}
