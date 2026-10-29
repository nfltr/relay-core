// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/quic-go"
	"github.com/nfltr/relay/client/agent"
)

// TestAgentLinkTCPSessions: on each transport, concurrent sessions of a
// TCP endpoint and in-process sessions (DialAgent) ride the agent link,
// intact.
func TestAgentLinkTCPSessions(t *testing.T) {
	for _, tc := range linkTransportAgents {
		t.Run(tc.transport, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, tc.ag)
			if err := echo(h, t, []byte("dial")); err != nil {
				t.Fatal(err)
			}
			waitLinkTransport(t, h, tc.transport)
			payload := bytes.Repeat([]byte("0123456789abcdef"), 8<<10)
			if err := runConcurrent(8, func() error {
				if err := echo(h, t, payload); err != nil {
					return err
				}
				if err := upload(h, t, 4<<20); err != nil {
					return err
				}
				return download(h, t, 4<<20)
			}); err != nil {
				t.Fatal(err)
			}
			if h.Streams() != 1 {
				t.Errorf("%d frame streams: the agent link should leave the primary alone", h.Streams())
			}
		})
	}
}

// TestAgentLinkE2EE: E2EE vhost sessions (TLS ended at the agent) on each
// transport.
func TestAgentLinkE2EE(t *testing.T) {
	body := bytes.Repeat([]byte("e2ee-body-"), 6000)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer backend.Close()
	for _, tc := range linkTransportAgents {
		t.Run(tc.transport, func(t *testing.T) {
			setup, err := agent.NewE2EESetup(harnessAgentID, "", "", slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			h := startTunnelHarnessWith(t, "", setup, backend.URL, 0, relayCurrent, tc.ag)
			tr := &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test-only self-signed agent cert
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", h.RelayLn.Addr().String())
				},
			}
			defer tr.CloseIdleConnections()
			client := &http.Client{Transport: tr, Timeout: 10 * time.Second}
			for i := range 5 {
				resp, err := client.Get("https://" + harnessAgentID + ".example/")
				if err != nil {
					t.Fatal(err)
				}
				got, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if !bytes.Equal(got, body) {
					t.Fatalf("body of %d bytes, want %d", len(got), len(body))
				}
				if i == 0 {
					waitLinkTransport(t, h, tc.transport)
					tr.CloseIdleConnections() // the rest on the link
				}
			}
		})
	}
}

// TestAgentLinkCutResetsVisitor: on each transport, a visitor downloading
// through the agent link when the link is lost reads a reset, never a
// clean end: a FIN would end its read at a fraction of the 1<<40 bytes it
// asked for, so the answer looks complete when it is not. link before
// v0.10.7 took a write into the lost link (a closed pipe without its
// cause; quic-go's connection errors, which are net.ErrClosed) for the
// agent's clean close. A download has no concurrent write to consume the
// reset, so the reset reaches the visitor's read on Linux too, where a
// socket error is delivered to one syscall only.
func TestAgentLinkCutResetsVisitor(t *testing.T) {
	for _, tc := range linkTransportAgents {
		t.Run(tc.transport, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, tc.ag)
			for i := range 20 {
				if err := echo(h, t, []byte("dial")); err != nil {
					t.Fatal(err)
				}
				waitLinkTransport(t, h, tc.transport)
				c := h.Dial(t)
				if _, err := c.Write(targetHeader('d', 1<<40)); err != nil {
					t.Fatal(err)
				}
				buf := make([]byte, 64<<10)
				if _, err := io.ReadFull(c, buf[:1]); err != nil {
					t.Fatalf("run %d: no download before the cut: %v", i, err)
				}
				time.Sleep(time.Duration(20+i*5) * time.Millisecond)
				cutLinks(h)
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				var err error
				for err == nil { // drain buffered bytes, then read the end
					_, err = c.Read(buf)
				}
				_ = c.Close()
				if err == io.EOF {
					t.Fatalf("run %d: the visitor read a clean end (its 1<<40-byte download complete) after the agent link was cut", i)
				}
			}
		})
	}
}

// waitLinkTransport waits for the agent link on want (it is dialled when
// the relay first needs a session).
func waitLinkTransport(t *testing.T, h *tunnelHarness, want string) {
	t.Helper()
	waitUntil(t, func() bool { return slices.Equal(linkTransports(h), []string{want}) },
		func() string { return fmt.Sprintf("agent link on %v, want %s", linkTransports(h), want) })
}

var linkTransportAgents = []struct {
	transport string
	ag        harnessAgent
}{{"tcp", agentCurrent}, {"quic", agentQUIC}}

// linkTransports are the transports of the agent-link connections bound on
// the harness relay.
func linkTransports(h *tunnelHarness) []string {
	h.D.Link.Mu.Lock()
	defer h.D.Link.Mu.Unlock()
	var ts []string
	for _, m := range h.D.Link.Conns {
		for _, c := range m {
			ts = append(ts, c.Transport)
		}
	}
	slices.Sort(ts)
	return slices.Compact(ts)
}

// cutLinks closes the relay's side of every agent-link connection, as a
// lost link (a dead agent, an idle timeout) ends it.
func cutLinks(h *tunnelHarness) {
	h.D.Link.Mu.Lock()
	var cs []io.Closer
	for _, m := range h.D.Link.Conns {
		for c := range m {
			cs = append(cs, c)
		}
	}
	h.D.Link.Mu.Unlock()
	for _, c := range cs {
		_ = c.Close()
	}
}

// TestAgentLinkSilentEndsPrimary: on each transport, an agent-link
// connection that ends silent past its liveness timeout ends the primary
// stream it is bound to (its frame streams share the dead path), so the
// relay routes no session there; any other end of a link connection does
// not.
func TestAgentLinkSilentEndsPrimary(t *testing.T) {
	for err, want := range map[error]bool{link.ErrDeadPeer: true, &quic.IdleTimeoutError{}: true, io.EOF: false, nil: false} {
		if linkSilent(err) != want {
			t.Errorf("linkSilent(%v) = %v, want %v", err, !want, want)
		}
	}
	for _, tc := range linkTransportAgents {
		t.Run(tc.transport, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, tc.ag)
			if err := echo(h, t, []byte("dial")); err != nil {
				t.Fatal(err)
			}
			waitLinkTransport(t, h, tc.transport)
			h.D.Mu.RLock()
			var token string
			for tk := range h.D.bound {
				token = tk
			}
			h.D.Mu.RUnlock()
			agentLinkObserver{l: h.D.Link}.ConnDown(link.Identity{Subject: token}, io.EOF)
			if h.Streams() == 0 {
				t.Fatal("a link connection's ordinary end ended the agent's primary")
			}
			h.D.Link.silent(token, tc.transport)
			waitUntil(t, func() bool {
				h.D.Mu.RLock()
				defer h.D.Mu.RUnlock()
				return h.D.bound[token] == nil
			}, func() string { return "the primary outlived its silent agent link" })
		})
	}
}
