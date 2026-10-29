// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/tls"
	"github.com/nfltr/relay-core/internal/platform/tcprig"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/client/grpcclient"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// e2eeVhostClient returns a client whose every connection is a new E2EE
// session through the harness relay.
func e2eeVhostClient(h *tunnelHarness, keepAlive bool) (*http.Client, *http.Transport) {
	tr := &http.Transport{
		DisableKeepAlives:   !keepAlive,
		MaxIdleConnsPerHost: 1,
		TLSClientConfig:     &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only self-signed agent cert
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", h.RelayLn.Addr().String())
		},
	}
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}, tr
}

// TestE2EEVhostReusesBackendConnections: concurrent E2EE sessions share the
// agent's connections to its backend. A transport per session (or one that
// keeps 2 idle connections) dialed the backend for most requests, which
// cost the agent a fifth of its CPU and filled its ports with TIME_WAIT.
func TestE2EEVhostReusesBackendConnections(t *testing.T) {
	h, dials := startE2EEVhost(t)
	const sessions, perSession = 32, 10
	var wg sync.WaitGroup
	var failed atomic.Int32
	for range sessions {
		wg.Go(func() {
			client, tr := e2eeVhostClient(h, true)
			defer tr.CloseIdleConnections()
			for range perSession {
				resp, err := client.Get("https://" + harnessAgentID + ".example/")
				if err != nil {
					failed.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		})
	}
	wg.Wait()
	if n := failed.Load(); n > 0 {
		t.Fatalf("%d requests failed", n)
	}
	// Not n <= sessions: net/http's Transport dials for a request that finds
	// no idle connection, and a connection whose response just ended is
	// idle only once its read loop returns it, which a loaded machine
	// delays past the session's next request; the dial completes and its
	// connection is kept (33 for 32 on the Linux runner, 145ac82). Without
	// reuse nearly every request dials.
	if n, reused := int(dials.Load()), sessions*perSession-sessions; n > sessions+reused/10 {
		t.Fatalf("%d backend connections for %d concurrent sessions (%d requests): the agent dials instead of reusing", n, sessions, sessions*perSession)
	}
}

// e2eeGet sends one GET with Connection: keep-alive (the stream path keeps
// the client's Connection header on its own backend connection, the reverse
// proxy drops it) and, when creds is set, Basic Auth.
func e2eeGet(client *http.Client, creds string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, "https://"+harnessAgentID+".example/", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Connection", "keep-alive")
	if user, pass, ok := strings.Cut(creds, ":"); ok {
		req.SetBasicAuth(user, pass)
	}
	return client.Do(req)
}

// TestE2EEVhostStreamPathByDefault: with no feature that reads requests, a
// session goes to the backend as a byte stream: Host is the one the client
// asked for (the agent does not rewrite it by default), the rest of the
// request as the client sent it but the forwarded and hop-by-hop headers.
func TestE2EEVhostStreamPathByDefault(t *testing.T) {
	h, _ := startE2EEVhost(t)
	client, tr := e2eeVhostClient(h, true)
	defer tr.CloseIdleConnections()
	resp, err := e2eeGet(client, "")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Host") != harnessAgentID+".example" {
		t.Fatalf("status %d, backend Host %q", resp.StatusCode, resp.Header.Get("X-Host"))
	}
	if resp.Header.Get("X-Hop") != "keep-alive" {
		t.Fatal("the request was rewritten on the way (reverse proxy), not streamed")
	}
}

// TestE2EEVhostBasicAuthForcesHTTPPath: --basic-auth must be enforced on
// E2EE sessions, where the agent is the only hop that reads requests. It
// takes the HTTP path: requests without the credentials get 401 and never
// reach the backend.
func TestE2EEVhostBasicAuthForcesHTTPPath(t *testing.T) {
	h, dials := startE2EEVhost(t, func(s *agent.E2EESetup) { s.RequireBasicAuth("alice:secret") })
	client, tr := e2eeVhostClient(h, true)
	defer tr.CloseIdleConnections()
	for _, creds := range []string{"", "alice:wrong"} {
		resp, err := e2eeGet(client, creds)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized || resp.Header.Get("Www-Authenticate") == "" {
			t.Fatalf("creds %q: status %d, want 401 with a challenge", creds, resp.StatusCode)
		}
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("unauthenticated requests reached the backend (%d connections)", n)
	}
	resp, err := e2eeGet(client, "alice:secret")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Header.Get("X-Hop") != "" {
		t.Fatalf("status %d, hop header %q: want 200 through the reverse proxy", resp.StatusCode, resp.Header.Get("X-Hop"))
	}
}

// BenchmarkE2EEVhostRequest: 1 KiB requests through the E2EE vhost, with a
// connection (TLS handshake to the agent) per request or kept alive, on the
// stream path (default) and the HTTP path (basic auth).
func BenchmarkE2EEVhostRequest(b *testing.B) {
	for _, path := range []string{"stream", "http"} {
		for _, keepAlive := range []bool{true, false} {
			name := path + "/keepalive"
			if !keepAlive {
				name = path + "/conn-per-request"
			}
			b.Run(name, func(b *testing.B) {
				creds := ""
				var configure []func(*agent.E2EESetup)
				if path == "http" {
					creds = "alice:secret"
					configure = append(configure, func(s *agent.E2EESetup) { s.RequireBasicAuth(creds) })
				}
				h, dials := startE2EEVhost(b, configure...)
				client, tr := e2eeVhostClient(h, keepAlive)
				defer tr.CloseIdleConnections()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					resp, err := e2eeGet(client, creds)
					if err != nil {
						b.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				b.ReportMetric(float64(dials.Load())/float64(b.N), "backend-dials/op")
			})
		}
	}
}

// TestTLSPassthroughReachesTheService: `nfltr http --mode passthrough` runs
// the agent's TCP tunnel with the service as its forward address and no E2EE
// termination. An SNI-routed session then reaches the HTTPS service as TLS
// bytes: the client's TLS session is with the service's certificate, so
// neither the relay nor the agent can read it.
func TestTLSPassthroughReachesTheService(t *testing.T) {
	svc := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "from the service")
	}))
	defer svc.Close()

	logger := slog.New(slog.DiscardHandler)
	d := NewTCPDispatcher(logger, nil, nil)
	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(append(grpcfactory.ProtocolOptions(0), grpcfactory.TransportOptions()...)...)
	pb.RegisterDispatcherServer(srv, tcprig.ConnectTCPStub(d, TCPBindHeader))
	go func() { _ = srv.Serve(grpcLn) }()
	defer srv.Stop()
	cc, err := grpc.NewClient(grpcLn.Addr().String(), append(grpcclient.ClientIdentity(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cc.Close() }()
	ctx := t.Context()
	go func() {
		_ = agent.RunTCPTunnel(ctx, pb.NewDispatcherClient(cc), agent.RelayDialer{}, ctx, svc.Listener.Addr().String(), logger)
	}()

	relayLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = relayLn.Close() }()
	go func() {
		for {
			c, err := relayLn.Accept()
			if err != nil {
				return
			}
			go d.RouteConnection(ctx, c, harnessAgentID, "", "")
		}
	}()

	client := svc.Client() // trusts the service's certificate only
	client.Transport.(*http.Transport).DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", relayLn.Addr().String())
	}
	resp, err := client.Get(svc.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "from the service" || resp.TLS == nil || !resp.TLS.PeerCertificates[0].Equal(svc.Certificate()) {
		t.Fatalf("body %q: want the service's own TLS session", body)
	}
}

// startE2EEVhost starts the harness with an E2EE agent (configure, when
// given, adjusts its setup) and a backend that answers 1 KiB and reports the
// Host and Connection request headers it saw (X-Host, X-Hop).
func startE2EEVhost(tb testing.TB, configure ...func(*agent.E2EESetup)) (*tunnelHarness, *atomic.Int32) {
	tb.Helper()
	var dials atomic.Int32
	backend := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Host", r.Host)
		w.Header().Set("X-Hop", r.Header.Get("Connection"))
		_, _ = w.Write(make([]byte, 1024))
	}))
	backend.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			dials.Add(1)
		}
	}
	backend.Start()
	tb.Cleanup(backend.Close)
	setup, err := agent.NewE2EESetup(harnessAgentID, "", "", slog.New(slog.DiscardHandler))
	if err != nil {
		tb.Fatal(err)
	}
	for _, c := range configure {
		c(setup)
	}
	return startTunnelHarness(tb, "", setup, backend.URL), &dials
}
