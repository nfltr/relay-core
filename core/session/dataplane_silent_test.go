// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/quic-go"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestGRPCPortClosesSilentConnections: a connection to the gRPC port that
// says nothing, or only the first bytes of a data connection or of the
// agent link and then nothing, is closed within its handshake bound; none
// holds a connection and a goroutine for as long as it likes (a relay bug
// class seen elsewhere: no handshake deadline once a connection is told
// apart). gRPC's own bound is grpcfactory's TestServerClosesAStalledHandshake.
func TestGRPCPortClosesSilentConnections(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := d.DataPlaneListener(inner)
	t.Cleanup(func() { _ = l.Close() }) // after the parallel subtests
	go func() {                         // the gRPC server's part: what reaches Accept is not silent here
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	agentLinkBound := d.Link.tcp.DeadAfter()
	for _, tc := range []struct {
		name  string
		first string
		bound time.Duration
	}{
		{"nothing", "", DataPlaneSniffTimeout},
		{"data connection", link.Magic, DataPlaneSniffTimeout},
		{"agent link", dataplane.AgentLinkMagic, agentLinkBound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, err := net.Dial("tcp", inner.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close() //nolint:errcheck
			if _, err := c.Write([]byte(tc.first)); err != nil {
				t.Fatal(err)
			}
			bound := tc.bound + 5*time.Second
			_ = c.SetReadDeadline(time.Now().Add(bound))
			start := time.Now()
			if _, err := io.Copy(io.Discard, c); errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("a silent connection still open after %v", bound)
			}
			t.Logf("closed after %v", time.Since(start).Round(time.Millisecond))
		})
	}
}

// TestAgentLinkQUICClosesSilentConnections: a QUIC connection to the agent
// link that opens no stream, or one that sends no bind hello, is closed
// within DataPlaneSniffTimeout, while the peer keeps the connection alive.
func TestAgentLinkQUICClosesSilentConnections(t *testing.T) {
	_, addr := qadListener(t)
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	for _, openStream := range []bool{false, true} {
		t.Run(map[bool]string{false: "no stream", true: "no hello"}[openStream], func(t *testing.T) {
			t.Parallel()
			pc, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			tr := &quic.Transport{Conn: pc}
			defer tr.Close()                                                                    //nolint:errcheck
			tc := &tls.Config{InsecureSkipVerify: true, NextProtos: dataplane.AgentLinkALPNs()} //nolint:gosec // test-only self-signed relay cert
			q, err := tr.Dial(t.Context(), ua, tc, &quic.Config{KeepAlivePeriod: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer q.CloseWithError(0, "") //nolint:errcheck
			if openStream {
				st, err := q.OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := st.Write([]byte{0}); err != nil { // the stream reaches the relay; no hello follows
					t.Fatal(err)
				}
			}
			bound := DataPlaneSniffTimeout + 5*time.Second
			start := time.Now()
			select {
			case <-q.Context().Done():
				t.Logf("closed after %v", time.Since(start).Round(time.Millisecond))
			case <-time.After(bound):
				t.Fatalf("a silent agent-link QUIC connection still open after %v", bound)
			}
		})
	}
}
