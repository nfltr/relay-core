// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
)

// startAgentHTTPLink runs the tunnel harness with an HTTP-only agent whose
// sessions reach backend, and the relay's leaf in front of it.
func startAgentHTTPLink(t *testing.T, backend string) (*tunnelHarness, string) {
	return startAgentHTTPLinkOn(t, backend, agentCurrent)
}

// startAgentHTTPLinkOn is startAgentHTTPLink with the agent ag.
func startAgentHTTPLinkOn(t *testing.T, backend string, ag harnessAgent) (*tunnelHarness, string) {
	h, leaf := startAgentHTTPLeaf(t, backend, ag)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(session.AgentIDKey, harnessAgentID)
		leaf.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return h, srv.URL
}

// startAgentHTTPLeaf runs the tunnel harness with an HTTP-only agent ag
// whose sessions reach backend, and returns the relay's leaf for it.
func startAgentHTTPLeaf(t testing.TB, backend string, ag harnessAgent) (*tunnelHarness, *agentHTTP) {
	viper.Set("http.server.max_request_body_size", int64(32<<20))
	viper.Set("default.request.timeout", 30*time.Second)
	t.Cleanup(viper.Reset)
	h := startHarness(t, 0, relayCurrent, ag, func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
		handler, err := agent.NewTunnelHandler(agent.Config{AgentID: harnessAgentID, Routes: []agent.Route{{Backend: backend}}, Logger: logger})
		if err != nil {
			t.Error(err)
			return
		}
		_ = agent.RunTunnel(ctx, client, dial, ctx, agent.Tunnel{HTTP: handler}, logger)
	}, func(context.Context, *session.TCPDispatcher, net.Conn) {})
	return h, newAgentHTTP(h.d, slog.New(slog.DiscardHandler), testRelayCookie)
}

// TestAgentHTTPLinkServesRequests: a generation-3 agent's requests go on
// an HTTP session, with what the gRPC path did to them: the relay's
// cookies and credential never reach the agent, the timing header is set,
// and the agent is not listed as a TCP agent.
func TestAgentHTTPLinkServesRequests(t *testing.T) {
	backend := startLinkHTTPBackend(t)
	h, url := startAgentHTTPLink(t, backend.URL)
	if !h.d.ServesHTTP(harnessAgentID) || slices.Contains(h.d.ConnectedAgentIDs(), harnessAgentID) {
		t.Fatalf("serves http %v, tcp agents %v", h.d.ServesHTTP(harnessAgentID), h.d.ConnectedAgentIDs())
	}
	req, _ := http.NewRequest(http.MethodPost, url+"/echo", strings.NewReader("hello"))
	req.Header.Set("Cookie", "rpc_session=secret; app=1")
	req.Header.Set("X-API-Key", "relay-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "5" || resp.Header.Get("X-Cookie") != "app=1" || resp.Header.Get("X-Api-Key-Seen") != "" {
		t.Fatalf("%d %q cookie %q key %q", resp.StatusCode, body, resp.Header.Get("X-Cookie"), resp.Header.Get("X-Api-Key-Seen"))
	}
	if resp.Header.Get(session.MetadataHeader) == "" || resp.Header.Get("Location") != "/moved" {
		t.Fatalf("metadata %q location %q", resp.Header.Get(session.MetadataHeader), resp.Header.Get("Location"))
	}
}

// TestAgentHTTPLinkBodies: a large upload and download stream through;
// a body over http.server.max_request_body_size gets 413, declared or not.
func TestAgentHTTPLinkBodies(t *testing.T) {
	backend := startLinkHTTPBackend(t)
	_, url := startAgentHTTPLink(t, backend.URL)
	const n = 24 << 20
	resp, err := http.Post(url+"/echo", "application/octet-stream", io.LimitReader(&patternReader{}, n))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != strconv.Itoa(n) {
		t.Fatalf("upload: %d %q", resp.StatusCode, body)
	}
	resp, err = http.Get(url + "/dl?n=" + strconv.Itoa(n))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if got != n {
		t.Fatalf("download: %d bytes", got)
	}
	for _, declared := range []bool{true, false} {
		req, _ := http.NewRequest(http.MethodPost, url+"/echo", io.LimitReader(&patternReader{}, 40<<20))
		if declared {
			req.ContentLength = 40 << 20
		} else {
			req.ContentLength = -1
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestEntityTooLarge {
			t.Errorf("declared %v: %d, want 413", declared, resp.StatusCode)
		}
	}
}

// TestAgentHTTPLinkStreamsAndCancels: a streamed response's first event
// arrives before the response ends, and a visitor that goes away ends the
// agent's request to its backend.
func TestAgentHTTPLinkStreamsAndCancels(t *testing.T) {
	backend := startLinkHTTPBackend(t)
	_, url := startAgentHTTPLink(t, backend.URL)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	if err != nil || line != "data: one\n" {
		t.Fatalf("first event %q %v", line, err)
	}
	cancel()
	_ = resp.Body.Close()

	ctx, cancel = context.WithCancel(t.Context())
	req, _ = http.NewRequestWithContext(ctx, http.MethodGet, url+"/slow", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = resp.Body.Close()
	select {
	case <-backend.cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("the backend request outlived its visitor")
	}
}

// TestAgentHTTPLinkStuckVisitorIsolation: a visitor that stops reading
// holds only its own session; eight others download meanwhile.
func TestAgentHTTPLinkStuckVisitorIsolation(t *testing.T) {
	backend := startLinkHTTPBackend(t)
	_, url := startAgentHTTPLink(t, backend.URL)
	const each = 16 << 20
	stuck, err := http.Get(url + "/dl?n=" + strconv.Itoa(each))
	if err != nil {
		t.Fatal(err)
	}
	defer stuck.Body.Close() //nolint:errcheck
	var wg sync.WaitGroup
	var ok atomic.Int32
	for range 8 {
		wg.Go(func() {
			resp, err := http.Get(url + "/dl?n=" + strconv.Itoa(each))
			if err != nil {
				return
			}
			n, _ := io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if n == each {
				ok.Add(1)
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("downloads beside a stuck visitor did not finish")
	}
	if ok.Load() != 8 {
		t.Fatalf("%d of 8 downloads intact", ok.Load())
	}
}

// linkHTTPBackend is what the tests' agent forwards to.
type linkHTTPBackend struct {
	*httptest.Server
	cancelled chan struct{} // a /slow request's context ended
}

func startLinkHTTPBackend(t testing.TB) *linkHTTPBackend {
	b := &linkHTTPBackend{cancelled: make(chan struct{}, 1)}
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/echo":
			w.Header().Set("X-Cookie", r.Header.Get("Cookie"))
			w.Header().Set("X-Api-Key-Seen", r.Header.Get("X-API-Key"))
			w.Header().Set("Location", "/moved")
			w.Header().Set("Content-Type", "application/octet-stream")
			n, _ := io.Copy(io.Discard, r.Body)
			_, _ = io.WriteString(w, strconv.FormatInt(n, 10))
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: one\n\n")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/slow":
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(http.StatusOK)
			// A first byte: the agent's proxy sends no response before its
			// body starts, and the relay would answer only at its request
			// timeout (default.request.timeout, 30 s here), cancelling the
			// backend itself, not because the visitor went away.
			_, _ = w.Write([]byte{0})
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			b.cancelled <- struct{}{}
		case "/dl":
			n, _ := strconv.Atoi(r.URL.Query().Get("n"))
			w.Header().Set("Content-Type", "application/octet-stream")
			_, _ = io.CopyN(w, &patternReader{}, int64(n))
		}
	}))
	t.Cleanup(b.Close)
	return b
}
