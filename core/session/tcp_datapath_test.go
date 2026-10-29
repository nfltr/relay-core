// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay/client/agent"
)

// Many sessions share an agent's streams: every byte must arrive intact and in
// order in both directions. This guards the agent's single-sender rule for
// its gRPC stream: with concurrent Send (unsafe in grpc-go) the sessions
// deadlocked in the transport's write quota. Run it under -race too.
func TestTCPTunnelConcurrentSessionsIntact(t *testing.T) {
	h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, agentDataConns)
	payload := bytes.Repeat([]byte("0123456789abcdef"), 16<<10) // 256 KiB
	done := make(chan error, 1)
	go func() {
		done <- runConcurrent(8, func() error {
			if err := echo(h, t, payload); err != nil {
				return err
			}
			if err := upload(h, t, 1<<20); err != nil {
				return err
			}
			return download(h, t, 1<<20)
		})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("concurrent sessions on one agent stream stalled")
	}
	// Concurrent sessions made the agent open the tcpAgentStreams the relay
	// asked for, each on a connection of its own (the further ones data
	// connections); new sessions take the data connections in turn.
	h.WaitStreams(t, tcpAgentStreams)
	h.WaitConns(t, tcpAgentStreams)
	seen := map[*tcpAgentStream]bool{}
	for range tcpAgentStreams - 1 {
		as := h.D.agentStream(harnessAgentID)
		if as.DC == nil {
			t.Fatal("a new session of a busy agent went to its gRPC stream, not a data connection")
		}
		seen[as] = true
	}
	if len(seen) != tcpAgentStreams-1 {
		t.Fatalf("%d sessions used %d data connections, want %d", tcpAgentStreams-1, len(seen), tcpAgentStreams-1)
	}
}

// An agent opens the further streams only once it is busy: an idle one, or
// one serving a session at a time, keeps one stream and one connection
// (each costs the relay ~40 KiB). The relay retires an auxiliary connection
// that idled (tcpAuxIdle), and a busy agent opens it again.
func TestTCPTunnelIdleAgentKeepsOneStream(t *testing.T) {
	shortAuxIdle(t)
	h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, agentDataConns)
	for range 3 {
		if err := echo(h, t, []byte("one at a time")); err != nil {
			t.Fatal(err)
		}
	}
	// Back to back sessions can overlap for a moment on the agent (the next
	// opens before the previous one's close arrived) and grow it; idle, it
	// is back to one stream on one connection.
	h.WaitStreams(t, 1)
	h.WaitConns(t, 1)
	for range 2 {
		a, b := h.Dial(t), h.Dial(t) // two at once: busy
		h.WaitStreams(t, tcpAgentStreams)
		h.WaitConns(t, tcpAgentStreams)
		_ = a.Close()
		_ = b.Close()
		// Idle again: the auxiliary connections close, the primary stays.
		h.WaitStreams(t, 1)
		h.WaitConns(t, 1)
		if err := echo(h, t, []byte("still here")); err != nil {
			t.Fatal(err)
		}
	}
}

// shortAuxIdle makes harness relays started after it retire idle auxiliary
// connections at once.
func shortAuxIdle(t *testing.T) {
	harnessAuxIdle = 50 * time.Millisecond
	t.Cleanup(func() { harnessAuxIdle = 0 })
}

// Interop: an agent before auxiliary connections (no dialer: it ignores the
// bind token) opens its further streams on its one connection, and a relay
// before them (no bind token, as rounds 2-3) gets them that way from a
// current agent. Sessions work either way.
func TestTCPTunnelStreamsOnOneConnWithoutBinding(t *testing.T) {
	for name, h := range map[string]*tunnelHarness{
		"older agent": startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, agentOneConn),
		"older relay": startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayStreamsOnly, agentCurrent),
	} {
		if err := runConcurrent(4, func() error { return upload(h, t, 1<<20) }); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		h.WaitStreams(t, tcpAgentStreams)
		if c := h.Conns.Load(); c != 1 {
			t.Fatalf("%s: %d streams on %d connections, want all on one", name, tcpAgentStreams, c)
		}
	}
}

// When the agent's primary stream ends (a relay restart, a reconnect), its
// auxiliary connections end with it on both sides: their sessions close,
// the agent closes the connections, and nothing of the agent is left to
// route a new session to.
func TestTCPTunnelAuxConnsEndWithPrimary(t *testing.T) {
	h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, agentDataConns)
	var held []net.Conn
	for range 2 * tcpAgentStreams { // sessions on every stream, held open
		c := h.Dial(t)
		defer c.Close() //nolint:errcheck
		if _, err := c.Write([]byte{'e'}); err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	h.WaitConns(t, tcpAgentStreams)
	close(h.EndPrimary)
	for i, c := range held {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		// Closed: EOF, or a reset (the relay closed with the echo unread).
		if _, err := io.Copy(io.Discard, c); errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("session %d still open after its agent's primary stream ended", i)
		}
	}
	h.WaitStreams(t, 0)
	h.WaitConns(t, 1) // the harness's own connection; the agent closed the rest
	if h.D.agentStream(harnessAgentID) != nil {
		t.Fatal("a new session would still be routed to the ended agent")
	}
}

// An agent keeps one stream with a relay that does not ask for more (the
// header is new): sessions work, and no extra streams appear once the
// relay's headers arrived with its first frame.
func TestTCPTunnelOneStreamWithOlderRelay(t *testing.T) {
	h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayOneStream, agentCurrent)
	if err := echo(h, t, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := runConcurrent(4, func() error { return upload(h, t, 1<<20) }); err != nil {
		t.Fatal(err)
	}
	if n, c := h.Streams(), h.Conns.Load(); n != 1 || c != 1 {
		t.Fatalf("agent opened %d streams on %d connections to a relay that asked for none", n, c)
	}
}

// A target that writes its data and closes must not lose its tail: the
// close frame trails the data frames on the stream, and the relay must
// write everything queued before closing the client conn.
func TestTCPTunnelCloseAfterDataDeliversEverything(t *testing.T) {
	h := startTunnelHarness(t, startTarget(t), nil, "")
	for i := range 20 {
		if err := download(h, t, 1<<20); err != nil {
			t.Fatalf("download %d: %v", i, err)
		}
	}
}

// Vhost E2EE sessions with one connection per request (no keep-alive):
// every response must arrive whole, and each finished session must release
// everything the agent started for it.
func TestTCPTunnelE2EEConnectionPerRequest(t *testing.T) {
	body := bytes.Repeat([]byte("e2ee-body-"), 6000) // ~60 KiB
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer backend.Close()

	setup, err := agent.NewE2EESetup(harnessAgentID, "", "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	shortAuxIdle(t) // back to back sessions may grow the agent for a moment
	h := startTunnelHarness(t, "", setup, backend.URL)
	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only self-signed agent cert
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", h.RelayLn.Addr().String())
		},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}

	get := func() error {
		resp, err := client.Get("https://" + harnessAgentID + ".example/")
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		got, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if !bytes.Equal(got, body) {
			return fmt.Errorf("body %d bytes, want %d", len(got), len(body))
		}
		return nil
	}
	if err := get(); err != nil { // warm up pools before the baseline
		t.Fatalf("warm-up request: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	const requests = 100
	failed := 0
	var firstErr error
	for range requests {
		if err := get(); err != nil {
			failed++
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if failed > 0 {
		t.Errorf("%d/%d connection-per-request vhost requests failed; first: %v", failed, requests, firstErr)
	}
	tr.CloseIdleConnections()
	backend.CloseClientConnections()
	if n, err := waitGoroutines(base+10, 5*time.Second); err != nil {
		t.Errorf("%d E2EE sessions left %d goroutines running (baseline %d)", requests, n-base, base)
	}
}

// Keep-alive vhost connections serve many requests each. The agent's
// session conn must stop http.Server's background read between requests
// (a read deadline in the past) and keep every response whole.
func TestTCPTunnelE2EEKeepAlive(t *testing.T) {
	body := bytes.Repeat([]byte("k"), 1024)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer backend.Close()
	setup, err := agent.NewE2EESetup(harnessAgentID, "", "", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	h := startTunnelHarness(t, "", setup, backend.URL)
	var dials atomic.Int64
	const conns, perConn = 8, 50
	// One client per connection, each allowed one: keep-alive must carry
	// all of its requests, so dials == conns exactly.
	err = runConcurrent(conns, func() error {
		tr := &http.Transport{
			MaxConnsPerHost: 1,
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only self-signed agent cert
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				dials.Add(1)
				return (&net.Dialer{}).DialContext(ctx, "tcp", h.RelayLn.Addr().String())
			},
		}
		defer tr.CloseIdleConnections()
		client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
		for range perConn {
			resp, err := client.Get("https://" + harnessAgentID + ".example/")
			if err != nil {
				return err
			}
			got, err := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err != nil {
				return err
			}
			if !bytes.Equal(got, body) {
				return fmt.Errorf("body %d bytes, want %d", len(got), len(body))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := dials.Load(); n != conns {
		t.Fatalf("%d requests on %d keep-alive clients took %d connections", conns*perConn, conns, n)
	}
}

// BenchmarkTCPTunnelThroughput moves data through relay -> gRPC -> agent ->
// target with 1 and 8 concurrent sessions per agent stream. MB/s is the
// aggregate; peak-heap-MiB is the live heap high-water mark of relay and
// agent together (both run in this process).
func BenchmarkTCPTunnelThroughput(b *testing.B) {
	const perSession = 8 << 20
	for _, dir := range []string{"upload", "download"} {
		for _, sessions := range []int{1, 8} {
			b.Run(fmt.Sprintf("%s/sessions=%d", dir, sessions), func(b *testing.B) {
				h := startTunnelHarness(b, startTarget(b), nil, "")
				move := upload
				if dir == "download" {
					move = download
				}
				b.SetBytes(int64(perSession * sessions))
				b.ReportAllocs()
				runtime.GC()
				hs := startHeapSampler()
				b.ResetTimer()
				for b.Loop() {
					if err := runConcurrent(sessions, func() error { return move(h, b, perSession) }); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(hs.Stop())/(1<<20), "peak-heap-MiB")
			})
		}
	}
}

// BenchmarkTCPTunnelThroughputRTT is one session over an agent connection
// with 40 ms RTT: throughput is bounded by the HTTP/2 flow-control windows
// (stream window / RTT), so it compares window settings.
func BenchmarkTCPTunnelThroughputRTT(b *testing.B) {
	const perSession = 32 << 20
	for _, dir := range []string{"upload", "download"} {
		b.Run(dir, func(b *testing.B) {
			h := startTunnelHarnessRTT(b, startTarget(b), nil, "", 40*time.Millisecond)
			move := upload
			if dir == "download" {
				move = download
			}
			b.SetBytes(perSession)
			for b.Loop() {
				if err := move(h, b, perSession); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
