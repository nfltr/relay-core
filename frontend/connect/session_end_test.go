// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay-core/platform/metrics"
)

// endRecorder is a link or quiclink session as a joint sees it (every end
// method peerlink.Join uses), recording the end calls a wrapper passes on.
type endRecorder struct {
	net.Conn
	calls []string
}

func (e *endRecorder) rec(m string) error             { e.calls = append(e.calls, m); return nil }
func (e *endRecorder) CloseWrite() error              { return e.rec("CloseWrite") }
func (e *endRecorder) CloseWriteSync() error          { return e.rec("CloseWriteSync") }
func (e *endRecorder) Reset() error                   { return e.rec("Reset") }
func (e *endRecorder) HoldEndAck()                    { _ = e.rec("HoldEndAck") }
func (e *endRecorder) AckEnd()                        { _ = e.rec("AckEnd") }
func (e *endRecorder) Close() error                   { return e.rec("Close") }
func (e *endRecorder) Done() <-chan struct{}          { return nil }
func (e *endRecorder) Answers() bool                  { return true }
func (e *endRecorder) Accept() error                  { return nil }
func (e *endRecorder) AcceptPath(uint8) error         { return nil }
func (e *endRecorder) Refuse(uint32, string) error    { return nil }
func (e *endRecorder) AbortWith(*link.Abort) error    { return e.rec("AbortWith") }
func (e *endRecorder) Answered(context.Context) error { return nil }
func (e *endRecorder) Chain() link.Chain              { return link.Chain{} }
func (e *endRecorder) PathLen() uint8                 { return 0 }
func (e *endRecorder) CloseWriteSyncReceived(received func()) error {
	received()
	return e.rec("CloseWriteSyncReceived")
}

// TestSessionWrappersPassTheEndOn (lane QUICEND; relay's AnswerConn class,
// relay client/nf1 TestAnswerConnPassesTheSessionsEnd): every wrapper the
// relay puts over a link or quiclink session on a join's path passes its
// end on: CloseWrite, CloseWriteSync, CloseWriteSyncReceived, Reset, and
// the joint's hold (HoldEndAck, AckEnd), and unwraps to the session
// (Unwrap() net.Conn, link peerlink.Join's doc; lane LINKWRAP: without it
// the joint takes a link session behind it for a plain conn, its end
// onto a QUIC far end passed unheld, its connection unwatched,
// TestCountedSessionJoinWatchesItsConnection). Hidden, the joint half-closes
// without waiting for the far end and never holds the end: an anonymous
// nf1 leg (budgeted) did. core/session TestNF1LegAnswersNotServedWhenTheAgentLeft
// checks nf1Leg, platform/metrics TestCountConnPassesTheEndHandshake
// CountConn on its own.
func TestSessionWrappersPassTheEndOn(t *testing.T) {
	leg := func() *nf1anon.Leg {
		return nf1anon.New(nf1anon.Settings{Enabled: true, LegBytes: 1 << 20, LegTime: time.Hour, BucketBytes: 1 << 20}, nil).Leg("nf1:00")
	}
	for _, tc := range []struct {
		name string
		wrap func(*endRecorder) net.Conn
	}{
		{"counted", func(e *endRecorder) net.Conn {
			return metrics.CountConn(e, metrics.PathConnect, nil, time.Now(), true, true)
		}},
		{"budgeted", func(e *endRecorder) net.Conn { return budgeted(newLocalEnd(), e, leg()) }},
		{"budgeted, counted", func(e *endRecorder) net.Conn {
			return metrics.CountConn(budgeted(newLocalEnd(), e, leg()), metrics.PathConnect, nil, time.Now(), true, true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &endRecorder{}
			w := tc.wrap(e)
			if !unwrapsTo(w, e) {
				t.Fatal("the wrapper does not unwrap to its session (Unwrap() net.Conn): a joint takes a link session behind it for a plain conn")
			}
			s, ok := w.(interface {
				CloseWrite() error
				CloseWriteSync() error
				CloseWriteSyncReceived(func()) error
				Reset() error
				peerlink.EndAcker
			})
			if !ok {
				t.Fatal("the wrapper hides its session's CloseWrite, CloseWriteSync, CloseWriteSyncReceived, Reset, HoldEndAck or AckEnd")
			}
			_ = s.CloseWrite()
			_ = s.CloseWriteSync()
			got := false
			_ = s.CloseWriteSyncReceived(func() { got = true })
			s.HoldEndAck()
			s.AckEnd()
			_ = s.Reset()
			want := []string{"CloseWrite", "CloseWriteSync", "CloseWriteSyncReceived", "HoldEndAck", "AckEnd", "Reset"}
			if !got || !slices.Equal(e.calls, want) {
				t.Fatalf("passed on %v (receipt %v), want %v", e.calls, got, want)
			}
		})
	}
}

// unwrapsTo reports whether c is inner or unwraps to it (peerlink.Join
// follows every Unwrap() net.Conn).
func unwrapsTo(c, inner net.Conn) bool {
	for c != inner {
		u, ok := c.(interface{ Unwrap() net.Conn })
		if !ok {
			return false
		}
		c = u.Unwrap()
	}
	return true
}

// TestCountedSessionJoinWatchesItsConnection (lane LINKWRAP; link
// spec/Join.tla Lost): serveHijacked's join, a client's TCP conn and the
// agent's link session behind metrics.CountConn. The agent half-closes,
// the client read it and sits idle, then the link connection dies: the
// joint must end the client. Behind a counted conn that hid the session
// (no Unwrap) the joint watched no connection and waited for ever on the
// idle client.
func TestCountedSessionJoinWatchesItsConnection(t *testing.T) {
	ln, rn := tcpPair(t)
	ctx := context.Background()
	go func() {
		c, _, err := link.Server(ctx, ln, link.Config{Auth: link.NoAuth})
		if err != nil {
			_ = ln.Close()
			return
		}
		_ = link.NewMux(c).Serve(func(_ []byte, s *link.Session) {
			_ = s.CloseWrite() // the agent's target half-closes
			_, _ = io.Copy(io.Discard, s)
		})
	}()
	rc, err := link.Client(ctx, rn, link.Config{})
	if err != nil {
		t.Fatal(err)
	}
	rm := link.NewMux(rc)
	go func() { _ = rm.Serve(nil) }()
	s, err := rm.Open(ctx, []byte("h"))
	if err != nil {
		t.Fatal(err)
	}
	client, relay := tcpPair(t)
	joinErr := make(chan error, 1)
	go func() {
		joinErr <- peerlink.Join(relay, metrics.CountConn(s, metrics.PathConnect, nil, time.Now(), true, true))
	}()
	_ = client.SetReadDeadline(time.Now().Add(10 * time.Second))
	if b, err := io.ReadAll(client); err != nil || len(b) != 0 {
		t.Fatalf("client read %q, %v; want EOF", b, err)
	}
	_ = ln.Close() // the link connection dies; the client stays idle
	select {
	case err := <-joinErr:
		if err == nil {
			t.Fatal("Join ended the client cleanly after its link connection died")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Join did not end the idle client after its link connection died: the counted conn hid the session")
	}
}

// tcpPair is two ends of a loopback TCP connection, closed at cleanup.
func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close() //nolint:errcheck
	ch := make(chan net.Conn, 1)
	go func() { c, _ := l.Accept(); ch <- c }()
	a, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b := <-ch
	if b == nil {
		t.Fatal("accept failed")
	}
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return a.(*net.TCPConn), b.(*net.TCPConn)
}
