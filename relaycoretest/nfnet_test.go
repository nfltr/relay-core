// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/nf1"
	"github.com/nfltr/relay/client/nfnet"
	"golang.org/x/net/nettest"
)

// The relay SDK's nfnet end to end through a relay core: Listen's agent
// and Dial's data connections meet here, so the SDK's Listener and Conn
// are checked against the relay they run on (nfltr
// docs/decisions/p2p-sdk-2026-11-14.md, section 3).

// sdk is a relay core with an nfnet listener of agent "chat", service
// "chat", whose agent reaches the relay through grpc (r.GRPC, or a proxy
// in front of it).
type sdk struct {
	r   *relaycoretest.Relay
	cfg nfnet.Config
	d   *nfnet.Dialer
}

func newSDK(t *testing.T, grpc func(*relaycoretest.Relay) string) *sdk {
	t.Helper()
	r := relaycoretest.New(t)
	agentRelay := r.GRPC
	if grpc != nil {
		agentRelay = grpc(r)
	}
	cfg := nfnet.Config{Relay: r.Proxy, AgentRelay: agentRelay, Agent: "chat", APIKey: r.Key, Plaintext: true}
	d, err := nfnet.NewDialer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return &sdk{r: r, cfg: cfg, d: d}
}

func (s *sdk) listen(t *testing.T, o nfnet.ListenOptions) *nfnet.Listener {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	l, err := nfnet.Listen(ctx, s.cfg, "chat", o)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// target is the listener's service as this key's clients dial it.
func (s *sdk) target() nfnet.Target {
	return nfnet.Target{Kind: nfnet.AgentTarget, Agent: s.r.Ref("chat"), Service: "chat"}
}

// dial dials the listener's service within d (held while its agent
// registers).
func (s *sdk) dial(t *testing.T, d time.Duration) (*nfnet.Conn, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	return s.d.DialTarget(ctx, s.target(), nfnet.DialOptions{})
}

// pair dials the listener and accepts that session.
func (s *sdk) pair(t *testing.T, l *nfnet.Listener) (*nfnet.Conn, net.Conn) {
	t.Helper()
	type res struct {
		c   *nfnet.Conn
		err error
	}
	dialed := make(chan res, 1)
	go func() { c, err := s.dial(t, 30*time.Second); dialed <- res{c, err} }()
	ac, err := l.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	d := <-dialed
	if d.err != nil {
		t.Fatalf("Dial: %v", d.err)
	}
	return d.c, ac
}

func wantCode(t *testing.T, what string, err error, code nfnet.Code) {
	t.Helper()
	if got := nfnet.CodeOf(err); got != code {
		t.Fatalf("%s: %v (code %v); want %v", what, err, got, code)
	}
}

// *nfnet.Conn is a correct net.Conn, dialled and accepted, through a relay
// core: deadlines (write deadlines too: a writer the peer does not read
// waits on the session's credit, then times out), concurrent Close, Read
// after Close (golang.org/x/net/nettest). Plain mode: under end-to-end TLS
// a write that timed out poisons the TLS stream, as crypto/tls documents,
// which nettest's resync after a timeout does not allow.
func TestConnIsANetConn(t *testing.T) {
	s := newSDK(t, nil)
	l := s.listen(t, nfnet.ListenOptions{})
	nettest.TestConn(t, func() (c1, c2 net.Conn, stop func(), err error) {
		c, ac := s.pair(t, l)
		return c, ac, func() { _ = c.Close(); _ = ac.Close() }, nil
	})
}

// Accept answers the open: a dial waits (held) while nobody accepts, is
// answered at Accept, and one never accepted is refused at Close
// (CodeRefused), never opened and then reset. The accepted Conn names the
// caller the relay authenticated.
func TestListenerAnswersAtAccept(t *testing.T) {
	s := newSDK(t, nil)
	l := s.listen(t, nfnet.ListenOptions{})
	c, ac := s.pair(t, l)
	if i := ac.(*nfnet.Conn).Info(); i.Caller == nil || i.Caller.Machine == "" {
		t.Fatalf("accepted Info %+v: no caller", i)
	}
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	_ = c.CloseWrite()
	if b, err := io.ReadAll(ac); err != nil || string(b) != "ping" {
		t.Fatalf("accepted read %q, %v", b, err)
	}
	_, _ = ac.Write([]byte("pong"))
	_ = ac.Close()
	if b, err := io.ReadAll(c); err != nil || string(b) != "pong" {
		t.Fatalf("dialled read %q, %v", b, err)
	}
	_ = c.Close()

	dialed := make(chan error, 1)
	go func() { _, err := s.dial(t, 30*time.Second); dialed <- err }()
	select {
	case err := <-dialed:
		t.Fatalf("a dial nobody accepted returned %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	_ = l.Close()
	wantCode(t, "a dial never accepted, at Close", <-dialed, nfnet.CodeRefused)
	if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after Close: %v", err)
	}
}

// The backlog bounds what waits: a session beyond it is refused at once.
func TestListenerBacklogRefuses(t *testing.T) {
	s := newSDK(t, nil)
	l := s.listen(t, nfnet.ListenOptions{Backlog: 1})
	c, ac := s.pair(t, l) // registered
	_ = c.Close()
	_ = ac.Close()
	held := make(chan error, 1)
	go func() { _, err := s.dial(t, 30*time.Second); held <- err }()
	time.Sleep(300 * time.Millisecond) // the first in the backlog
	_, err := s.dial(t, 10*time.Second)
	wantCode(t, "beyond the backlog", err, nfnet.CodeRefused)
	if _, err := l.Accept(); err != nil {
		t.Fatal(err)
	}
	if err := <-held; err != nil {
		t.Fatalf("the session within the backlog: %v", err)
	}
}

// Shutdown refuses new sessions and waits for the accepted ones; past its
// ctx it resets them. Close resets the accepted at once.
func TestListenerShutdown(t *testing.T) {
	s := newSDK(t, nil)
	l := s.listen(t, nfnet.ListenOptions{})
	c, ac := s.pair(t, l)
	done := make(chan error, 1)
	go func() { done <- l.Shutdown(context.Background()) }()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		_, err := s.dial(t, 5*time.Second)
		if nfnet.CodeOf(err) == nfnet.CodeRefused {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("a dial during Shutdown: %v; want CodeRefused", err)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned %v with a session open", err)
	default:
	}
	_, _ = ac.Write([]byte("bye"))
	_ = ac.Close()
	if b, err := io.ReadAll(c); err != nil || string(b) != "bye" {
		t.Fatalf("read %q, %v", b, err)
	}
	_ = c.Close() // else the agent's drain waits for this half of the session
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	s2 := newSDK(t, nil)
	l2 := s2.listen(t, nfnet.ListenOptions{})
	c2, _ := s2.pair(t, l2)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := l2.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown past ctx: %v", err)
	}
	_ = c2.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, err := c2.Read(make([]byte, 1))
	wantCode(t, "a session Shutdown cut", err, nfnet.CodeReset)
}

// A relay restart (the agent's connection cut): the accepted Conn on the
// lost link reads a reset, and the listener accepts again once its agent
// redialled.
func TestListenerAcrossRelayRestart(t *testing.T) {
	var p *switchProxy
	s := newSDK(t, func(r *relaycoretest.Relay) string { p = newSwitchProxy(t, r.GRPC); return p.ln.Addr().String() })
	l := s.listen(t, nfnet.ListenOptions{})
	c, ac := s.pair(t, l)
	p.Switch(s.r.GRPC)
	_ = ac.SetReadDeadline(time.Now().Add(15 * time.Second))
	if _, err := ac.Read(make([]byte, 1)); err == nil || err == io.EOF {
		t.Fatalf("accepted Conn after its link was lost: %v; want a reset", err)
	}
	_ = c.Close()
	// The relay core answers an agent between its connections not found
	// (no directory to say offline), which a dial does not hold: dial
	// until the agent redialled.
	accepted := make(chan net.Conn, 1)
	go func() {
		if ac, err := l.Accept(); err == nil {
			accepted <- ac
		}
	}()
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		c, err := s.dial(t, 5*time.Second)
		if err == nil {
			_, _ = c.Write([]byte("again"))
			ac = <-accepted
			b := make([]byte, 5)
			if _, err := io.ReadFull(ac, b); err != nil || string(b) != "again" {
				t.Fatalf("after the restart: %q, %v", b, err)
			}
			_ = c.Close()
			_ = ac.Close()
			return
		}
		if nfnet.CodeOf(err) != nfnet.CodeNotFound || time.Now().After(deadline) {
			t.Fatalf("dial after the restart: %v", err)
		}
	}
}

// A listener under an nf1 address: a client with no credential (an empty
// Config but the relay's address) dials the address; the session is the
// address's pinned TLS, half closes both ways.
func TestListenerNF1Address(t *testing.T) {
	enableNF1(t, 64)
	s := newSDK(t, nil)
	nl, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	l := s.listen(t, nfnet.ListenOptions{Address: nl})
	if l.Address() == "" || l.Address() != nl.Address().String() {
		t.Fatalf("Address %q", l.Address())
	}
	anon, err := nfnet.NewDialer(nfnet.Config{Plaintext: true, DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", s.r.Proxy)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer anon.Close() //nolint:errcheck
	dialed := make(chan *nfnet.Conn, 1)
	go func() {
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			c, err := anon.Dial(ctx, l.Address(), nfnet.DialOptions{})
			cancel()
			if err == nil {
				dialed <- c
				return
			}
		}
		dialed <- nil
	}()
	ac, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	c := <-dialed
	if c == nil {
		t.Fatal("no nf1 session")
	}
	if !ac.(*nfnet.Conn).Info().E2EE || !c.Info().E2EE {
		t.Fatal("an nf1 session that is not end to end encrypted")
	}
	_, _ = c.Write([]byte("secret"))
	_ = c.CloseWrite()
	if b, err := io.ReadAll(ac); err != nil || string(b) != "secret" {
		t.Fatalf("accepted read %q, %v", b, err)
	}
	_ = ac.Close()
	if b, err := io.ReadAll(c); err != nil || len(b) != 0 {
		t.Fatalf("dialled read %q, %v", b, err)
	}
	_ = c.Close()
}
