// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package publictcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

func TestParseRange(t *testing.T) {
	for in, want := range map[string][2]int{"20000-20999": {20000, 20999}, "25565": {25565, 25565}, " 1024 - 1030 ": {1024, 1030}} {
		lo, hi, err := ParseRange(in)
		if err != nil || lo != want[0] || hi != want[1] {
			t.Errorf("ParseRange(%q) = %d, %d, %v", in, lo, hi, err)
		}
	}
	for _, in := range []string{"", "a-b", "20999-20000", "0-10", "65000-65536", "1-2-3", "-5"} {
		if lo, hi, err := ParseRange(in); err == nil {
			t.Errorf("ParseRange(%q) = %d, %d, want an error", in, lo, hi)
		}
	}
	if err := CheckEphemeral(20000, 20999, 32768, 60999); err != nil {
		t.Errorf("below the ephemeral range refused: %v", err)
	}
	if err := CheckEphemeral(30000, 33000, 32768, 60999); err == nil {
		t.Error("a range overlapping the ephemeral range accepted")
	}
}

// The range is bound whole or not at all: a port another process holds
// turns the feature off, and nothing of the range stays bound.
func TestBindAllOrNone(t *testing.T) {
	lns, err := Bind("127.0.0.1", 0, 0)
	if err == nil {
		closeAll(lns)
		t.Fatal("port 0 bound")
	}
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	p := busy.Addr().(*net.TCPAddr).Port
	lo, hi := p-2, p+2
	lns, err = Bind("127.0.0.1", lo, hi)
	if err == nil {
		closeAll(lns)
		t.Fatalf("bound %d-%d over a held port %d", lo, hi, p)
	}
	for q := lo; q <= hi; q++ {
		if q == p {
			continue
		}
		l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(q)))
		if err != nil {
			// Another process may hold a neighbour; the point is ours are freed.
			continue
		}
		l.Close()
	}
}

// fixture runs a Server on one loopback listener whose port maps through
// grants; Route echoes the visitor's bytes (the agent stand-in).
type fixture struct {
	t      *testing.T
	srv    *Server
	port   int
	mu     sync.Mutex
	grants map[int]Grant
	reads  atomic.Int32
	block  chan struct{} // when set, the next lookup waits on it
	logs   *syncWriter
	cancel context.CancelFunc
	done   chan struct{}
}

func newFixture(t *testing.T, mod func(*Config)) *fixture {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, port: ln.Addr().(*net.TCPAddr).Port, grants: map[int]Grant{}, logs: &syncWriter{w: &bytes.Buffer{}}, done: make(chan struct{})}
	cfg := Config{
		Lookup: func(port int) (Grant, error) {
			f.reads.Add(1)
			f.mu.Lock()
			g, ok := f.grants[port]
			b := f.block
			f.block = nil
			f.mu.Unlock()
			if b != nil {
				<-b
			}
			if !ok {
				return Grant{}, ErrNoGrant
			}
			return g, nil
		},
		Route: func(_ context.Context, c net.Conn, _ Grant) {
			_, _ = io.Copy(c, c)
		},
		Log:         slog.New(slog.NewTextHandler(f.logs, nil)),
		IdleTimeout: time.Minute,
	}
	if mod != nil {
		mod(&cfg)
	}
	f.srv = New(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go func() { _ = f.srv.Serve(ctx, []net.Listener{ln}); close(f.done) }()
	t.Cleanup(func() { cancel(); <-f.done; f.srv.Wait() })
	return f
}

func (f *fixture) set(g Grant) {
	f.mu.Lock()
	f.grants[f.port] = g
	f.mu.Unlock()
}

func (f *fixture) dial() net.Conn {
	f.t.Helper()
	c, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.port)))
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { c.Close() })
	return c
}

// echoes reports whether c echoes a probe within a second.
func echoes(c net.Conn) bool {
	_ = c.SetDeadline(time.Now().Add(time.Second))
	defer c.SetDeadline(time.Time{}) //nolint:errcheck
	if _, err := c.Write([]byte("ping")); err != nil {
		return false
	}
	b := make([]byte, 4)
	_, err := io.ReadFull(c, b)
	return err == nil && string(b) == "ping"
}

// closed waits (bounded) for c to end.
func closed(c net.Conn) bool {
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err := c.Read(make([]byte, 1))
	return err != nil && !isTimeout(err)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestNoGrantClosed(t *testing.T) {
	f := newFixture(t, nil)
	if !closed(f.dial()) {
		t.Fatal("a port with no lease stayed open")
	}
}

func TestRoutesAndAudits(t *testing.T) {
	f := newFixture(t, nil)
	f.set(Grant{ShareID: "abcd0123abcd0123", AgentID: "a1", Service: "mc"})
	c := f.dial()
	if !echoes(c) {
		t.Fatal("not routed")
	}
	c.Close()
	waitFor(t, func() bool { return strings.Contains(f.logText(), "public tcp: visit ended") })
	line := f.logText()
	for _, want := range []string{"share=abcd0123abcd0123", "port=", "remote=127.0.0.1:", "bytes_in=4", "bytes_out=4", "duration=", "end="} {
		if !strings.Contains(line, want) {
			t.Errorf("audit line lacks %q: %s", want, line)
		}
	}
	if strings.Contains(line, "ping") {
		t.Errorf("audit line carries payload: %s", line)
	}
}

func TestAllowlist(t *testing.T) {
	f := newFixture(t, nil)
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc", Policy: frontend.SharePolicy{AllowedIPs: []string{"192.0.2.0/24"}}})
	if !closed(f.dial()) {
		t.Fatal("a source outside the allowlist was routed")
	}
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc", Policy: frontend.SharePolicy{AllowedIPs: []string{"127.0.0.0/8"}}})
	f.srv.Cut("s1") // the policy change: the cached grant forgotten
	if !echoes(f.dial()) {
		t.Fatal("an allowed source refused")
	}
}

func TestEndpointCap(t *testing.T) {
	f := newFixture(t, nil)
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc", MaxConns: 1})
	c := f.dial()
	if !echoes(c) {
		t.Fatal("first visit refused")
	}
	if !closed(f.dial()) {
		t.Fatal("a visit over the endpoint's cap routed")
	}
	c.Close()
	waitFor(t, func() bool { return f.srv.Visits("s1") == 0 })
	if !echoes(f.dial()) {
		t.Fatal("a visit after one left refused")
	}
}

func TestSourceCap(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.Source.MaxPerIP = 1 })
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc"})
	c := f.dial()
	if !echoes(c) {
		t.Fatal("first visit refused")
	}
	if !closed(f.dial()) {
		t.Fatal("a second connection from one source routed over source_max")
	}
}

// The journal's end of a lease (Cut): its held visits end, its grant is
// forgotten, and a new visit reads the store again.
func TestCutEndsVisitsAndForgets(t *testing.T) {
	f := newFixture(t, nil)
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc"})
	c := f.dial()
	if !echoes(c) {
		t.Fatal("not routed")
	}
	f.mu.Lock()
	delete(f.grants, f.port)
	f.mu.Unlock()
	if n := f.srv.Cut("s1"); n != 1 {
		t.Fatalf("Cut ended %d visits, want 1", n)
	}
	if !closed(c) {
		t.Fatal("a held visit outlived its lease's end")
	}
	if !closed(f.dial()) {
		t.Fatal("a cut lease admitted a new visit")
	}
}

// A lookup that an end raced is not cached (spec PortLease.tla
// LookupStore, mutant lookup-race).
func TestLookupRacingCutNotCached(t *testing.T) {
	f := newFixture(t, nil)
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc"})
	b := make(chan struct{})
	f.mu.Lock()
	f.block = b
	f.mu.Unlock()
	c := f.dial()
	waitFor(t, func() bool { return f.reads.Load() == 1 })
	f.srv.Cut("s1") // applied while the lookup is in flight
	close(b)
	// The racing visit itself is refused: its grant was read before the end.
	if !closed(c) {
		t.Fatal("a visit admitted on a grant read before its lease's end")
	}
	before := f.reads.Load()
	if !echoes(f.dial()) {
		t.Fatal("next visit refused")
	}
	if f.reads.Load() == before {
		t.Fatal("the raced grant was cached: the next visit read no row")
	}
}

// A hard end (no payment method) cuts held visits at the end by the
// relay's own timer and refuses visits after it, with no journal entry
// (spec NoVisitPastLifetime).
func TestHardEnd(t *testing.T) {
	f := newFixture(t, nil)
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc", HardEnd: time.Now().Add(400 * time.Millisecond)})
	c := f.dial()
	if !echoes(c) {
		t.Fatal("not routed before its end")
	}
	if !closed(c) {
		t.Fatal("a visit outlived its hard end")
	}
	if !closed(f.dial()) {
		t.Fatal("a visit after the hard end routed")
	}
}

func TestIdleTimeout(t *testing.T) {
	f := newFixture(t, func(c *Config) { c.IdleTimeout = 300 * time.Millisecond })
	f.set(Grant{ShareID: "s1", AgentID: "a1", Service: "mc"})
	c := f.dial()
	if !echoes(c) {
		t.Fatal("not routed")
	}
	if !closed(c) {
		t.Fatal("an idle visit was not ended")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (f *fixture) logText() string {
	f.logs.mu.Lock()
	defer f.logs.mu.Unlock()
	return f.logs.w.String()
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// Only a plain tcp service the machine declared public is routed: the
// machine's own opt-in, beside the account's lease.
func TestDeclaredPublic(t *testing.T) {
	decls, err := agentservices.Parse("mc=tcp+plain;public,db=tcp+plain,web=http")
	if err != nil {
		t.Fatal(err)
	}
	for svc, want := range map[string]bool{"mc": true, "db": false, "web": false, "none": false} {
		if got := declaredPublic(decls, svc); got != want {
			t.Errorf("declaredPublic(%q) = %v, want %v", svc, got, want)
		}
	}
}
