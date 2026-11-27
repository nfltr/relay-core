// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

// The keep-alive pool across a replaced primary or QUIC connection
// (spec/HTTPPool.tla, NoStaleDispatch): the next request goes on what the
// directory names for the agent now, never on a pooled session of what it
// replaced.

// namedBackend answers every request with name.
func namedBackend(t *testing.T, name string) string {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, name) }))
	t.Cleanup(srv.Close)
	return srv.URL
}

// leafServer serves leaf for the harness agent.
func leafServer(t *testing.T, leaf *agentHTTP) func() (string, error) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(session.AgentIDKey, harnessAgentID)
		leaf.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	// Well within the replaced connection's liveness (quiclink DeadAfter,
	// 15 s): a request on it would wait for that.
	client := &http.Client{Timeout: 5 * time.Second}
	return func() (string, error) {
		resp, err := client.Get(srv.URL)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close() //nolint:errcheck
		b, err := io.ReadAll(resp.Body)
		return string(b), err
	}
}

// TestAgentHTTPLinkNewPrimaryTakesTheNextRequest: a newer primary of the
// agent (a restart, a takeover; here the older process lives on) takes
// the next request; the pool never reuses the older one's session
// (HTTPPool.tla mutant pool-by-agent: the code before).
func TestAgentHTTPLinkNewPrimaryTakesTheNextRequest(t *testing.T) {
	viper.Set("default.request.timeout", 30*time.Second)
	t.Cleanup(viper.Reset)
	first, second := namedBackend(t, "first"), namedBackend(t, "second")
	start := make(chan struct{})
	h := startHarness(t, 0, relayCurrent, agentCurrent, func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
		run := func(backend string) {
			handler, err := agent.NewTunnelHandler(agent.Config{AgentID: harnessAgentID, Routes: []agent.Route{{Backend: backend}}, Logger: logger})
			if err == nil {
				_ = agent.RunTunnel(ctx, client, dial, ctx, agent.Tunnel{HTTP: handler}, logger)
			}
		}
		go run(first)
		select {
		case <-start:
			run(second)
		case <-ctx.Done():
		}
	}, func(context.Context, *session.TCPDispatcher, net.Conn) {})
	waitUntil(t, func() bool { return h.d.LinkUp(harnessAgentID) }, func() string { return "the agent link did not come up" })
	get := leafServer(t, newAgentHTTP(h.d, slog.New(slog.DiscardHandler), testRelayCookie))
	if got, err := get(); got != "first" {
		t.Fatalf("got %q, %v", got, err)
	}
	h.d.Mu.RLock()
	old := h.d.Streams[harnessAgentID]
	h.d.Mu.RUnlock()
	close(start)
	waitUntil(t, func() bool {
		h.d.Mu.RLock()
		cur := h.d.Streams[harnessAgentID]
		h.d.Mu.RUnlock()
		return cur != old && h.d.LinkUp(harnessAgentID)
	}, func() string { return "the newer primary did not register" })
	for range 3 {
		if got, err := get(); got != "second" {
			t.Fatalf("a request after the newer primary registered reached %q (%v), want second", got, err)
		}
	}
}

// TestAgentHTTPLinkQUICReplacedTakesTheNextRequest: the agent's QUIC
// connection ends without a word reaching the relay (a closed socket; the
// LINKFAULTS partition) and the agent dials another: the next request
// goes on it at once, not on a pooled session of the old one, which the
// relay held until its liveness (HTTPPool.tla mutant no-retire).
func TestAgentHTTPLinkQUICReplacedTakesTheNextRequest(t *testing.T) {
	h, leaf := startAgentHTTPLeaf(t, namedBackend(t, "agent"), agentQUICCut)
	conns := func() map[any]string {
		h.d.Link.Mu.Lock()
		defer h.d.Link.Mu.Unlock()
		m := map[any]string{}
		for _, cs := range h.d.Link.Conns {
			for c, lc := range cs {
				m[c] = lc.Transport
			}
		}
		return m
	}
	waitUntil(t, func() bool { return len(conns()) == 1 }, func() string { return "no QUIC link connection" })
	before := conns()
	for _, tr := range before {
		if tr != dataplane.AgentLinkQUIC {
			t.Fatalf("link on %s, want quic", tr)
		}
	}
	get := leafServer(t, leaf)
	if got, err := get(); got != "agent" {
		t.Fatalf("got %q, %v", got, err)
	}
	h.cutQUIC()
	// A session open makes the agent write (its acknowledgement): the
	// write fails, quic-go drops the connection unseen, the agent dials
	// another.
	if c, err := h.d.DialAgent(context.Background(), harnessAgentID, dataplane.HTTPSessionTarget); err == nil {
		_ = c.Close()
	}
	waitUntil(t, func() bool {
		now := conns()
		maps.DeleteFunc(now, func(c any, _ string) bool { _, old := before[c]; return old })
		return len(now) > 0
	}, func() string { return "the agent dialled no new QUIC connection" })
	for range 3 {
		if got, err := get(); got != "agent" {
			t.Fatalf("a request after the agent replaced its connection: %q, %v", got, err)
		}
	}
}
