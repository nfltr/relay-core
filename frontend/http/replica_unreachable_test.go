// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/expiring"
	"github.com/spf13/viper"
)

func noRouteErr() error {
	return fmt.Errorf("%w: %w", &link.Abort{Refused: true, Code: link.CodeNoRoute,
		Msg: "agent far is connected to another replica of the relay, which could not be reached; try again"}, errors.New("connection refused"))
}

func wantReplicaUnreachable(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "another replica") {
		t.Fatalf("got %d Retry-After=%q %q, want 503 with Retry-After: the replica is unreachable, the agent is not offline", w.Code, w.Header().Get("Retry-After"), w.Body.String())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// A request forwarded to the agent's replica that the replica link cannot
// reach is 503 with Retry-After, not 502 "bad gateway".
func TestPeerProxyUnreachableReplicaIs503(t *testing.T) {
	pc := &proxyCache{transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, noRouteErr() }),
		cache: expiring.New[string, *httputil.ReverseProxy](1, 0, nil)}
	u, _ := url.Parse("http://10.0.0.9:8443")
	w := httptest.NewRecorder()
	pc.Get(u).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	wantReplicaUnreachable(t, w)
}

type linkOpener func(context.Context, session.TCPEndpoint) (session.PeerSession, error)

func (f linkOpener) OpenAgent(ctx context.Context, ep session.TCPEndpoint) (session.PeerSession, error) {
	return f(ctx, ep)
}

// An HTTP session whose agent turns out to be on a replica the link cannot
// reach is 503 with Retry-After, not 502 "Agent not connected".
func TestAgentHTTPUnreachableReplicaIs503(t *testing.T) {
	viper.Set("default.request.timeout", 30*time.Second)
	t.Cleanup(viper.Reset)
	td := session.NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, directory.NewNoOpInventory())
	td.Links = linkOpener(func(context.Context, session.TCPEndpoint) (session.PeerSession, error) { return nil, noRouteErr() })
	a := newAgentHTTP(td, slog.New(slog.DiscardHandler), func(string) bool { return false })
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set(session.AgentIDKey, "far")
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	wantReplicaUnreachable(t, w)
}
