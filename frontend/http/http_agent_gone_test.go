// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
)

// An HTTP response whose agent goes away mid-body (2026-10-15, 3-replica
// relay on Kubernetes: the agent closed 0.5 s after its backend flushed
// 128 KiB) must end at once and as an error at the visitor, on every path:
// same replica and across the replica link, on the gRPC stream and on agent
// link sessions. Before the fix the gRPC path left the visitor waiting for
// the request timeout (hang), then ended the chunked body cleanly (a
// truncated body that looks complete).

func grpcLeaf(t *testing.T, backend string) (http.Handler, func()) {
	h, a := startGRPCHTTPHandler(t, backend)
	return h, a.Close
}

func linkLeaf(ag harnessAgent) goneLeaf {
	return func(t *testing.T, backend string) (http.Handler, func()) {
		alive, kill := context.WithCancel(context.Background())
		t.Cleanup(kill)
		h := startHarness(t, 0, relayCurrent, ag, func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
			handler, err := agent.NewTunnelHandler(agent.Config{AgentID: harnessAgentID, Routes: []agent.Route{{Backend: backend}}, Logger: logger})
			if err != nil {
				t.Error(err)
				return
			}
			ctx, stop := context.WithCancel(ctx)
			defer stop()
			context.AfterFunc(alive, stop)
			_ = agent.RunTunnel(ctx, client, dial, ctx, agent.Tunnel{HTTP: handler}, logger)
		}, func(context.Context, *session.TCPDispatcher, net.Conn) {})
		waitUntil(t, func() bool { return h.d.ServesHTTP(harnessAgentID) }, func() string { return "the agent serves no HTTP sessions" })
		leaf := newAgentHTTP(h.d, slog.New(slog.DiscardHandler), testRelayCookie)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Header.Set(session.AgentIDKey, harnessAgentID)
			leaf.ServeHTTP(w, r)
		}), kill
	}
}

var goneLeaves = []struct {
	name string
	leaf goneLeaf
}{{"grpc", grpcLeaf}, {"link-tcp", linkLeaf(agentCurrent)}, {"link-quic", linkLeaf(agentQUIC)}}

var goneRoutes = []struct {
	name  string
	route func(*testing.T, http.Handler) string
}{{"same-replica", sameReplica}}

// TestHTTPResponseCutWhenAgentGoes: the agent closes mid-body; the
// visitor's body ends within a few seconds as an error.
func TestHTTPResponseCutWhenAgentGoes(t *testing.T) {
	for _, l := range goneLeaves {
		for _, r := range goneRoutes {
			t.Run(fmt.Sprintf("%s/%s", l.name, r.name), func(t *testing.T) {
				t.Cleanup(viper.Reset)
				owner, kill := l.leaf(t, goneBackend(t, nil))
				viper.Set("default.request.timeout", 60*time.Second) // the cut must not wait for it
				visitCut(t, r.route(t, owner), "/kill/0", kill, 5*time.Second)
			})
		}
	}
}

// TestHTTPResponseCutOnAgentIdle: the agent stays but sends nothing for
// the request timeout mid-body; the body ends as an error, never cleanly.
func TestHTTPResponseCutOnAgentIdle(t *testing.T) {
	for _, l := range goneLeaves {
		for _, r := range goneRoutes {
			t.Run(fmt.Sprintf("%s/%s", l.name, r.name), func(t *testing.T) {
				t.Cleanup(viper.Reset)
				owner, _ := l.leaf(t, goneBackend(t, nil))
				viper.Set("default.request.timeout", time.Second)
				visitCut(t, r.route(t, owner), "/stall", func() {}, 5*time.Second)
			})
		}
	}
}

// TestHTTPResponseRefusedWhenAgentGoesBeforeIt: the agent closes before its
// response started; the visitor gets 502 at once, not the request timeout.
func TestHTTPResponseRefusedWhenAgentGoesBeforeIt(t *testing.T) {
	for _, l := range goneLeaves {
		for _, r := range goneRoutes {
			t.Run(fmt.Sprintf("%s/%s", l.name, r.name), func(t *testing.T) {
				t.Cleanup(viper.Reset)
				late := make(chan struct{}, 1)
				owner, kill := l.leaf(t, goneBackend(t, late))
				viper.Set("default.request.timeout", 60*time.Second)
				base := r.route(t, owner)
				// Killed once the request reached the backend: killed before
				// (a loaded host), the relay rightly answers 503 offline.
				start := time.Now()
				go func() {
					select {
					case <-late:
						kill()
					case <-t.Context().Done():
					}
				}()
				resp, err := (&http.Client{Timeout: 20 * time.Second}).Get(base + "/late")
				if err != nil {
					t.Fatal(err)
				}
				_ = resp.Body.Close()
				if took := time.Since(start); resp.StatusCode != http.StatusBadGateway || took > 5*time.Second {
					t.Fatalf("status %d after %v, want 502 within 5s", resp.StatusCode, took.Round(time.Millisecond))
				}
			})
		}
	}
}

const (
	goneHead  = 128 << 10
	goneTotal = 1 << 20
)

// goneBackend writes goneHead bytes, flushes, and the rest 2 s later (by
// then its agent is gone); /stall never writes the rest; /late answers
// nothing for 2 s.
// goneBackend's /late tells late (when not nil) it arrived.
func goneBackend(t *testing.T, late chan<- struct{}) string {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/late" { // nothing until its agent is gone
			select {
			case late <- struct{}{}:
			default:
			}
			select {
			case <-time.After(2 * time.Second):
			case <-r.Context().Done():
			}
			return
		}
		_, _ = w.Write(make([]byte, goneHead))
		w.(http.Flusher).Flush()
		wait := 2 * time.Second
		if r.URL.Path == "/stall" {
			wait = time.Hour
		}
		select {
		case <-time.After(wait):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(make([]byte, goneTotal-goneHead))
	}))
	t.Cleanup(s.Close)
	return s.URL
}

// goneLeaf is a relay's leaf for an agent with a route to backend, and what
// kills that agent.
type goneLeaf func(t *testing.T, backend string) (http.Handler, func())

// sameReplica serves owner as the visitor's replica.
func sameReplica(t *testing.T, owner http.Handler) string {
	s := httptest.NewServer(owner)
	t.Cleanup(s.Close)
	return s.URL
}

// visitCut gets path, runs cut once the head arrived, and fails unless the
// body ends with an error within bound (not a hang, not a clean end).
func visitCut(t *testing.T, base, path string, cut func(), bound time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 20 * time.Second}
	var resp *http.Response
	for attempt := 1; ; attempt++ {
		var err error
		if resp, err = client.Get(base + path); err != nil {
			t.Fatal(err)
		}
		// 504: the response did not start within default.request.timeout
		// (the product's bound on the agent's first byte, 1 s here; a
		// loaded host misses it). The cut under test is mid-body: visit
		// again until the head came in time.
		if resp.StatusCode != http.StatusGatewayTimeout || attempt == 5 {
			break
		}
		_ = resp.Body.Close()
		t.Logf("visit %d: the response did not start within the request timeout (504)", attempt)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if _, err := io.ReadFull(resp.Body, make([]byte, goneHead)); err != nil {
		t.Fatalf("head: %v", err)
	}
	start := time.Now()
	time.AfterFunc(500*time.Millisecond, cut)
	n, err := io.Copy(io.Discard, resp.Body)
	took := time.Since(start)
	switch {
	case err == nil:
		t.Fatalf("clean end of a truncated body: %d more bytes after the head, want an error", n)
	case took > bound:
		t.Fatalf("hang: the body ended after %v (%v), want within %v", took.Round(time.Millisecond), err, bound)
	case !errors.Is(err, io.ErrUnexpectedEOF):
		t.Fatalf("body ended with %v, want unexpected EOF (a cut connection)", err)
	}
}
