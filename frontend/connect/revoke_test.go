// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestRevokedSessionsEndTyped: a data connection's open sessions end with
// an abort of reason ERROR_REASON_UNAUTHENTICATED when the connection's
// credential is revoked or expires (the client reads why, instead of a
// bare reset it would retry), and are left alone when the hold ended for
// any other cause (the client left).
func TestRevokedSessionsEndTyped(t *testing.T) {
	for _, tc := range []struct {
		cause error
		typed bool
	}{{identity.ErrRevoked, true}, {identity.ErrExpired, true}, {context.Canceled, false}} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		var open sessionSet
		accepted := make(chan struct{})
		served := make(chan struct{})
		relayEnd := make(chan error, 1) // what the relay's own side of the session reads: what its join passes on to the agent
		go func() {
			defer close(served)
			nc, err := ln.Accept()
			_ = ln.Close()
			if err != nil {
				return
			}
			defer nc.Close() //nolint:errcheck
			if _, err := http.ReadRequest(bufio.NewReader(nc)); err != nil {
				return
			}
			m, err := dataplane.Accept(t.Context(), nc, 1<<20, "allow")
			if err != nil {
				return
			}
			_ = m.Serve(func(_ *pb.TCPData, s *dataplane.Session) {
				open.add(s)
				accepted <- struct{}{}
				_, err := io.Copy(io.Discard, s)
				relayEnd <- err
			})
		}()
		nc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		m, _, err := dataplane.Upgrade(t.Context(), nc, "relay", "k", 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = m.Serve(nil) }()
		s, err := m.Open(&pb.TCPData{AgentId: "a", TargetAddr: "22"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		<-accepted

		open.abort(credentialAbort(tc.cause))
		wait := 200 * time.Millisecond // long enough for an abort that should not come
		if tc.typed {
			wait = 5 * time.Second
		}
		_ = s.SetReadDeadline(time.Now().Add(wait))
		_, err = s.Read(make([]byte, 1))
		switch {
		case tc.typed && !unauthenticated(err):
			t.Errorf("%v: the session ended with %v, want an abort of reason ERROR_REASON_UNAUTHENTICATED", tc.cause, err)
		case !tc.typed && !errors.Is(err, context.DeadlineExceeded) && !isTimeout(err):
			t.Errorf("%v: the session ended with %v, want it left open", tc.cause, err)
		}
		if tc.typed {
			// the join reads the relay's side: a clean end there closed the
			// agent's leg cleanly (link before lane LINKABORT)
			if err := <-relayEnd; !unauthenticated(err) {
				t.Errorf("%v: the relay's side of the session read %v, want the abort it sent", tc.cause, err)
			}
		}
		_ = m.Close()
		<-served
	}
}

// unauthenticated reports whether err is an abort of reason
// ERROR_REASON_UNAUTHENTICATED.
func unauthenticated(err error) bool {
	a, ok := errors.AsType[*link.Abort](err)
	reason, _ := dataplane.AbortReason(a)
	return ok && reason == relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// gatedConn holds its writes while armed: a write in flight on the
// relay's side of a data connection.
type gatedConn struct {
	net.Conn
	armed   atomic.Bool
	entered chan struct{} // a held write began
	release chan struct{} // it may go on
}

func (c *gatedConn) Write(b []byte) (int, error) {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		<-c.release
	}
	return c.Conn.Write(b)
}

// TestCutSessionsReadTheReasonBeforeTheClose (lane BILLABORT): a data
// connection whose credential's hold ends (a revocation, an expiry, an
// exhausted balance) ends each open session with its typed abort, and the
// client reads that abort before the connection's close, also when a
// write was in flight as the cut came (link's flush then leaves the
// ABORT buffered for that write's next turn; a close at once dropped it,
// relay-server TestBillingRefusalTyped read a bare EOF on a Linux CI run).
func TestCutSessionsReadTheReasonBeforeTheClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	gc := &gatedConn{entered: make(chan struct{}), release: make(chan struct{})}
	var open sessionSet
	accepted := make(chan *dataplane.Session, 1)
	var relay *dataplane.Mux
	ready := make(chan struct{})
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			close(ready)
			return
		}
		if _, err := http.ReadRequest(bufio.NewReader(nc)); err != nil {
			close(ready)
			return
		}
		gc.Conn = nc
		relay, err = dataplane.Accept(t.Context(), gc, 1<<20, "allow")
		close(ready)
		if err != nil {
			return
		}
		_ = relay.Serve(func(_ *pb.TCPData, s *dataplane.Session) {
			open.add(s)
			accepted <- s
		})
	}()
	nc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := dataplane.Upgrade(t.Context(), nc, "relay", "k", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close() //nolint:errcheck
	go func() { _ = m.Serve(nil) }()
	<-ready
	if relay == nil {
		t.Fatal("the relay's side did not accept")
	}
	s, err := m.Open(&pb.TCPData{AgentId: "a", TargetAddr: "22"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rs := <-accepted

	gc.armed.Store(true)
	go func() { _, _ = rs.Write([]byte("y")) }() // the write in flight
	<-gc.entered
	cut := make(chan struct{})
	go func() { defer close(cut); endData(relay, &open, identity.ErrBalanceExhausted) }()
	select { // the cut closes once its frames are written: it cannot return before the release
	case <-cut:
	case <-time.After(200 * time.Millisecond):
	}
	close(gc.release)

	_ = s.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = io.ReadAll(s)
	a, ok := errors.AsType[*link.Abort](err)
	if reason, _ := dataplane.AbortReason(a); !ok || reason != relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED {
		t.Fatalf("the cut session ended with %v, want reason ERROR_REASON_BALANCE_EXHAUSTED", err)
	}
	<-cut
}
