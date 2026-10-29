// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/peerlinktest"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/quic-go"
	"github.com/nfltr/relay-core/internal/platform/testcert"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// agentLinkTestToken binds the conformance pairs' agent (a primary stream
// that exists only in the dispatcher's table).
const agentLinkTestToken = "conformance-token"

func newTestAgentLink(t *testing.T) *agentLink {
	t.Helper()
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	d.bound[agentLinkTestToken] = &tcpAgentStream{agent: "agent", token: agentLinkTestToken}
	return newAgentLink(d, &net.TCPAddr{})
}

// TestAgentLinkConformanceTCP runs peerlink's suite with the relay's TCP
// agent-link Node as A (it opens sessions on the connections B dialled)
// and an agent's Node as B: the magic after the dial, the bind token in
// the hello, liveness fast.
func TestAgentLinkConformanceTCP(t *testing.T) {
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		l := newTestAgentLink(t)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() { // as route does on the gRPC port, deadline included:
					// a connection the proxy cut or black-holed before the
					// magic must not block here past the test (leakcheck)
					_ = c.SetReadDeadline(time.Now().Add(DataPlaneSniffTimeout))
					head := make([]byte, len(dataplane.AgentLinkMagic))
					if _, err := io.ReadFull(c, head); err != nil || string(head) != dataplane.AgentLinkMagic {
						_ = c.Close()
						return
					}
					_ = c.SetReadDeadline(time.Time{})
					l.serveConn(c)
				}()
			}
		}()
		px := peerlinktest.NewProxy(t, ln.Addr().String())
		b := mustNode(peerlink.New(peerlink.Config{
			Dial: func(ctx context.Context, _ string) (net.Conn, error) {
				nc, err := (&net.Dialer{}).DialContext(ctx, "tcp", px.Addr())
				if err == nil {
					_, err = nc.Write([]byte(dataplane.AgentLinkMagic))
				}
				return nc, err
			},
			Connect: []string{"relay"},
			Link:    link.Config{Auth: dataplane.BindToken(agentLinkTestToken), Liveness: link.LivenessFast},
			Pool:    link.PoolConfig{Min: 1},
		}))
		return peerlinktest.Pair{
			A: l.tcp, B: b, AName: "relay", BName: agentLinkTestToken, Reverse: true,
			DeadAfter: b.DeadAfter(),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

// TestAgentLinkConformanceQUIC is the suite with the relay's QUIC Node as
// A (peers named by the hello on their first stream) and an agent's
// quiclink Node as B.
func TestAgentLinkConformanceQUIC(t *testing.T) {
	cert := testcert.Generate(t)
	peerlinktest.Run(t, func(t *testing.T) peerlinktest.Pair {
		l := newTestAgentLink(t)
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pc.Close() })
		if err := l.listenQUIC(pc, &tls.Config{Certificates: []tls.Certificate{cert}}); err != nil {
			t.Fatal(err)
		}
		px := peerlinktest.NewUDPProxy(t, pc.LocalAddr().String())
		b := mustNode(quiclink.New(quiclink.Config{
			Dial: func(ctx context.Context, _ string, qc *quic.Config) (*quic.Conn, error) {
				tc := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{dataplane.AgentLinkALPN}} //nolint:gosec // test-only self-signed relay cert
				return dialAgentLinkQUIC(ctx, px.Addr(), tc, qc, agentLinkTestToken)
			},
			Connect:  []string{"relay"},
			Liveness: link.LivenessFast,
		}))
		return peerlinktest.Pair{
			A: l.quic, B: b, AName: "relay", BName: agentLinkTestToken, Reverse: true,
			DeadAfter: max(l.quic.DeadAfter(), b.DeadAfter()),
			BlackHole: px.BlackHole, Cut: px.Cut, Throttle: px.Throttle,
		}
	})
}

// dialAgentLinkQUIC dials as the agent does: QUIC, then the token's hello
// on the first stream.
func dialAgentLinkQUIC(ctx context.Context, addr string, tc *tls.Config, qc *quic.Config, token string) (*quic.Conn, error) {
	q, err := quic.DialAddr(ctx, addr, tc, qc)
	if err != nil {
		return nil, err
	}
	st, err := q.OpenStreamSync(ctx)
	if err == nil {
		_ = st.SetDeadline(time.Now().Add(5 * time.Second))
		err = dataplane.WriteLinkHello(st, token)
		_ = st.Close()
	}
	if err != nil {
		_ = q.CloseWithError(0, "")
		return nil, err
	}
	return q, nil
}

// TestAgentLinkRefusesUnknownToken: neither transport binds a token no
// primary issued.
func TestAgentLinkRefusesUnknownToken(t *testing.T) {
	l := newTestAgentLink(t)
	if _, err := l.verify("nope"); err == nil {
		t.Fatal("unknown token verified")
	}
	if _, err := (agentLinkTokens{l}).Verify(context.Background(), tls.ConnectionState{}, link.MethodToken, []byte("nope")); err == nil {
		t.Fatal("TCP: unknown token verified")
	}
	cert := testcert.Generate(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.listenQUIC(pc, &tls.Config{Certificates: []tls.Certificate{cert}}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = l.quic.Serve(refuseAgentSessions) }()
	t.Cleanup(func() {
		done, cancel := context.WithCancel(context.Background())
		cancel()
		_ = l.quic.Drain(done)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{dataplane.AgentLinkALPN}} //nolint:gosec // test-only self-signed relay cert
	if _, err := dialAgentLinkQUIC(ctx, pc.LocalAddr().String(), tc, l.quic.QUICConfig(), "nope"); err == nil {
		t.Fatal("QUIC: unknown token bound")
	}
}
