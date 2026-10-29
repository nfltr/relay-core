// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"
)

// What reaches an agent from a visitor's request: the relay's own
// X-Forwarded-For/Host/Proto (never the visitor's copies), and Origin and
// Referer untouched, on every way a request reaches the agent.

const fwdPublicHost = "app.share.example"

// visitorRequest is a visitor's request to fwdPublicHost that claims
// another host, scheme and address in the proxy headers.
func visitorRequest(tlsOn bool) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "http://"+fwdPublicHost+"/form", nil)
	r.RemoteAddr = "203.0.113.7:51000"
	if tlsOn {
		r.TLS = &tls.ConnectionState{}
	}
	r.Header.Set("Origin", "https://"+fwdPublicHost)
	r.Header.Set("Referer", "https://"+fwdPublicHost+"/page?x=1")
	r.Header.Set("X-Forwarded-Host", "evil.example")
	r.Header.Set("X-Forwarded-For", "198.51.100.1")
	r.Header.Set("X-Forwarded-Proto", "http")
	r.Header.Set("Forwarded", "for=198.51.100.1;host=evil.example")
	r.Header.Set("X-Real-Ip", "198.51.100.1")
	return r
}

// wantForwarded are the headers the agent must see for visitorRequest.
func wantForwarded(proto string) map[string]string {
	return map[string]string{
		"X-Forwarded-Host": fwdPublicHost, "X-Forwarded-Proto": proto, "X-Forwarded-For": "203.0.113.7",
		"Forwarded": "", "X-Real-Ip": "",
		"Origin": "https://" + fwdPublicHost, "Referer": "https://" + fwdPublicHost + "/page?x=1",
	}
}

func checkForwarded(t *testing.T, got http.Header, want map[string]string) {
	t.Helper()
	for k, v := range want {
		if vs := got.Values(k); (v == "" && len(vs) != 0) || (v != "" && (len(vs) != 1 || vs[0] != v)) {
			t.Errorf("%s = %q, want %q", k, vs, v)
		}
	}
}

// TestFrontSetsForwardedHeaders: the edge (front, the first handler of
// every agent request) replaces the visitor's proxy headers with its own.
func TestFrontSetsForwardedHeaders(t *testing.T) {
	ing := newTestIngress(&session.Core{})
	for _, tc := range []struct {
		tls   bool
		proto string
	}{{false, "http"}, {true, "https"}} {
		var got http.Header
		ing.front(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() })).
			ServeHTTP(httptest.NewRecorder(), visitorRequest(tc.tls))
		checkForwarded(t, got, wantForwarded(tc.proto))
	}
}

// TestForwardedHeadersReachTheAgent: the edge's headers reach the agent as
// set, on ConnectHTTP (gRPC), on an HTTP session (link) and through a peer
// replica's forward.
func TestForwardedHeadersReachTheAgent(t *testing.T) {
	want := wantForwarded("https")
	edge := func() *http.Request {
		r := visitorRequest(true)
		setForwardedHeaders(r)
		return r
	}

	t.Run("grpc", func(t *testing.T) {
		msg, _, err := prepareRequest(edge(), "a", "txn", time.Now().Add(time.Minute), time.Now())
		if err != nil {
			t.Fatal(err)
		}
		got := http.Header{}
		for k, v := range msg.Request.Headers {
			got[k] = v.HeaderValues
		}
		checkForwarded(t, got, want)
	})

	t.Run("link", func(t *testing.T) {
		seen := make(chan http.Header, 1)
		echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen <- r.Header.Clone()
		})
		viper.Set("http.server.max_request_body_size", int64(32<<20))
		viper.Set("default.request.timeout", 30*time.Second)
		t.Cleanup(viper.Reset)
		h := startHarness(t, 0, relayCurrent, agentCurrent, func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
			_ = agent.RunTunnel(ctx, client, dial, ctx, agent.Tunnel{HTTP: echo}, logger)
		}, func(context.Context, *session.TCPDispatcher, net.Conn) {})
		leaf := newAgentHTTP(h.d, slog.New(slog.DiscardHandler), testRelayCookie)
		r := edge()
		r.Header.Set(session.AgentIDKey, harnessAgentID)
		rec := httptest.NewRecorder()
		leaf.ServeHTTP(rec, r)
		select {
		case got := <-seen:
			checkForwarded(t, got, want)
		default:
			t.Fatalf("the agent got no request: %d %s", rec.Code, rec.Body)
		}
	})

	t.Run("peer", func(t *testing.T) {
		seen := make(chan *http.Request, 1)
		peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen <- r }))
		defer peer.Close()
		u, _ := url.Parse(peer.URL)
		newProxyCache(slog.New(slog.DiscardHandler)).Get(u).ServeHTTP(httptest.NewRecorder(), edge())
		r := <-seen
		checkForwarded(t, r.Header, want)
		if r.Host != fwdPublicHost {
			t.Errorf("peer got Host %q, want %q", r.Host, fwdPublicHost)
		}
	})
}
