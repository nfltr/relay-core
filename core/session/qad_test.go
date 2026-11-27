// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/nfltr/quic-go"
	"github.com/nfltr/relay/proto/wire/dataplane"

	"github.com/nfltr/relay-core/internal/platform/testcert"
)

// qadListener serves the agent link's QUIC Node on a loopback UDP port.
func qadListener(t *testing.T) (*agentLink, string) {
	t.Helper()
	l := newTestAgentLink(t)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := l.listenQUIC(pc, &tls.Config{Certificates: []tls.Certificate{testcert.Generate(t)}}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = l.quic.Serve(refuseAgentSessions) }()
	t.Cleanup(func() {
		done, cancel := context.WithCancel(context.Background())
		cancel()
		_ = l.quic.Drain(done)
	})
	return l, pc.LocalAddr().String()
}

// askQAD asks addr for the caller's observed address on tr's socket, as
// an endpoint does: ALPN qadALPN, read the stream, close.
func askQAD(ctx context.Context, tr *quic.Transport, addr string) (string, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return "", err
	}
	tc := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{qadALPN}} //nolint:gosec // test-only self-signed relay cert
	q, err := tr.Dial(ctx, ua, tc, &quic.Config{})
	if err != nil {
		return "", err
	}
	defer q.CloseWithError(0, "") //nolint:errcheck
	st, err := q.AcceptUniStream(ctx)
	if err != nil {
		return "", err
	}
	b, err := io.ReadAll(io.LimitReader(st, 64))
	return string(b), err
}

// TestQADAnswersObservedAddress: a QAD connection gets the address its
// packets came from (here the caller's own socket, no NAT between) and
// binds nothing on the agent link.
func TestQADAnswersObservedAddress(t *testing.T) {
	l, addr := qadListener(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// One Transport per socket, as an endpoint's; asked twice from it: the
	// same answer.
	tr := &quic.Transport{Conn: pc}
	defer tr.Close() //nolint:errcheck
	for range 2 {
		got, err := askQAD(ctx, tr, addr)
		if err != nil {
			t.Fatal(err)
		}
		if want := pc.LocalAddr().String(); got != want {
			t.Fatalf("observed %q, want %q", got, want)
		}
		if _, err := netip.ParseAddrPort(got); err != nil {
			t.Fatalf("answer %q is not an address: %v", got, err)
		}
	}
	l.Mu.Lock()
	n := len(l.Conns)
	l.Mu.Unlock()
	if n != 0 {
		t.Fatalf("a QAD connection bound %d agent-link connections", n)
	}
}

// TestQADLeavesAgentLink: the agent link's own ALPN still binds on the
// port that answers QAD (its token's hello, as before).
func TestQADLeavesAgentLink(t *testing.T) {
	l, addr := qadListener(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tc := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{dataplane.AgentLinkALPN}} //nolint:gosec // test-only self-signed relay cert
	q, err := dialAgentLinkQUIC(ctx, addr, tc, l.quic.QUICConfig(), agentLinkTestToken)
	if err != nil {
		t.Fatal(err)
	}
	defer q.CloseWithError(0, "") //nolint:errcheck
	if got := q.ConnectionState().TLS.NegotiatedProtocol; got != dataplane.AgentLinkALPN {
		t.Fatalf("ALPN %q, want %q", got, dataplane.AgentLinkALPN)
	}
	short, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	defer stop()
	_, err = q.AcceptUniStream(short) // no QAD answer on an agent-link connection
	if err == nil {
		t.Fatal("an agent-link connection got a QAD stream")
	}
}

// TestObservedAddrUnmaps: an IPv4 caller on a dual-stack socket is named
// by its IPv4 address; a non-UDP address is refused.
func TestObservedAddrUnmaps(t *testing.T) {
	got, err := observedAddr(&net.UDPAddr{IP: net.ParseIP("::ffff:203.0.113.5"), Port: 4242})
	if err != nil || got.String() != "203.0.113.5:4242" {
		t.Fatalf("observedAddr = %v, %v", got, err)
	}
	got, err = observedAddr(&net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 4242})
	if err != nil || got.String() != "[2001:db8::1]:4242" {
		t.Fatalf("observedAddr = %v, %v", got, err)
	}
	if _, err := observedAddr(&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}); err == nil {
		t.Fatal("a TCP address answered")
	}
}
