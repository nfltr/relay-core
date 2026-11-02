// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// nextFrame is the next frame the relay queued for the agent that match
// takes; it fails the test after a bound against a hang.
func nextFrame(t *testing.T, a *tcpAgentStream, what string, match func(*pb.TCPData) bool) *pb.TCPData {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case f := <-a.SendCh:
			if match(f) {
				return f
			}
		case <-deadline:
			t.Fatalf("the relay sent no %s", what)
			return nil
		}
	}
}

func isHalf(f *pb.TCPData) bool { return f.GetClose() && f.GetHalf() }

// lateResetConn is a visitor's connection whose abort (resetConn) lands
// late, as on a loaded machine: whatever else the relay does to the
// connection first reaches the client first.
type lateResetConn struct{ *net.TCPConn }

func (c lateResetConn) Reset() error {
	time.Sleep(50 * time.Millisecond)
	_ = c.SetLinger(0)
	return c.Close()
}

// endpointSession serves a visitor's TCP connection on a with
// handleLocalConn (the agent played by the test through a.SendCh and
// Dispatch; its abort lands late: lateResetConn); the client writes "tail"
// and, when half, half-closes. It returns the client's connection and the
// session's id once its "tail" (and half close) was sent.
func endpointSession(t *testing.T, a *tcpAgentStream) (net.Conn, string) {
	t.Helper()
	return endpointSessionHalf(t, a, true)
}

func endpointSessionHalf(t *testing.T, a *tcpAgentStream, half bool) (net.Conn, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	sc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		lc := lateResetConn{sc.(*net.TCPConn)}
		_ = d.handleLocalConn(lc, TCPEndpoint{AgentID: "agent"}, a)
		_ = lc.Close()
	}()
	t.Cleanup(func() { a.CloseAll(); <-done })
	sid := nextFrame(t, a, "open", (*pb.TCPData).GetOpen).GetSessionId()
	if _, err := c.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if !half {
		nextFrame(t, a, "tail", func(f *pb.TCPData) bool { return len(f.GetData()) > 0 })
		return c, sid
	}
	_ = c.(*net.TCPConn).CloseWrite()
	nextFrame(t, a, "half close", isHalf)
	return c, sid
}

// clientEnd is how the client's read side ends within d: nil while it has
// not, else the read's error (io.EOF for a clean end).
func clientEnd(c net.Conn, d time.Duration) error {
	_ = c.SetReadDeadline(time.Now().Add(d))
	_, err := io.Copy(io.Discard, c)
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return nil
	}
	if err == nil {
		return io.EOF
	}
	return err
}

// TestTCPTunnelEndAckStreamLossResetsHalfClosedClient: a client that
// half-closed and whose agent's half close crossed its own (the relay has
// both, the agent has not yet acked the client's) must not get a clean
// end: its tail may still be on the way. Once the stream ends there it is
// reset (spec/README.md finding 2, race:clean-tail-lost); an in-process
// reader gets errTCPSessionLost. Before, both halves ended the session at
// once: the client read EOF.
func TestTCPTunnelEndAckStreamLossResetsHalfClosedClient(t *testing.T) {
	t.Run("endpoint", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		c, sid := endpointSession(t, a)
		a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0) // crossed the client's
		if f := nextFrame(t, a, "ack", func(f *pb.TCPData) bool { return f.GetSessionId() == dataplane.EndAckSID }); string(f.GetData()) != sid {
			t.Fatalf("ack of %q, want %q", f.GetData(), sid)
		}
		if err := clientEnd(c, 100*time.Millisecond); err != nil {
			t.Fatalf("the client's read ended before its half close was acked: %v", err)
		}
		a.CloseAll() // the stream ended
		if err := clientEnd(c, 10*time.Second); err == nil || err == io.EOF {
			t.Fatalf("the client's read after the stream ended: %v, want a reset", err)
		}
	})
	// Neither side closed: the loss is the client's only end, though the
	// relay's writer returns before the abort (its removeSession waits for
	// CloseAll's).
	t.Run("endpoint-open", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		c, _ := endpointSessionHalf(t, a, false)
		a.CloseAll()
		if err := clientEnd(c, 10*time.Second); err == nil || err == io.EOF {
			t.Fatalf("the client's read after the stream ended: %v, want a reset", err)
		}
	})
	t.Run("in-process", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		c := inProcSession(t, a)
		a.Dispatch(&pb.TCPData{SessionId: "s", Close: true, Half: true}, 0)
		a.CloseAll()
		if _, err := c.Read(make([]byte, 1)); !errors.Is(err, errTCPSessionLost) {
			t.Fatalf("read after the stream ended: %v, want errTCPSessionLost", err)
		}
	})
}

// inProcSession is an in-process session "s" on a whose conn wrote "tail"
// and half-closed.
func inProcSession(t *testing.T, a *tcpAgentStream) *inProcConn {
	t.Helper()
	sess := newTCPSession()
	sess.endAck = a.endAck
	a.addSession("s", sess)
	c := &inProcConn{agentStream: a, sessionID: "s", sess: sess, cleanupFn: func() { a.removeSession("s"); sess.markClosed() }}
	if _, err := c.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	nextFrame(t, a, "half close", isHalf)
	return c
}

// TestTCPTunnelEndAckEndsCleanOnceAcked: with the ack the session ends
// cleanly: the client reads EOF, an in-process reader io.EOF.
func TestTCPTunnelEndAckEndsCleanOnceAcked(t *testing.T) {
	t.Run("endpoint", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		c, sid := endpointSession(t, a)
		a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0)
		a.recvAck(dataplane.EndAckFrame(sid))
		if err := clientEnd(c, 10*time.Second); err != io.EOF {
			t.Fatalf("the client's read: %v, want EOF", err)
		}
	})
	t.Run("in-process", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		c := inProcSession(t, a)
		a.Dispatch(&pb.TCPData{SessionId: "s", Close: true, Half: true}, 0)
		a.recvAck(dataplane.EndAckFrame("s"))
		a.CloseAll()
		if _, err := c.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("read: %v, want io.EOF", err)
		}
	})
}

// TestTCPTunnelEndAckOldAgentEndsAtOnce: on a stream without the end
// acknowledgement (an agent that does not announce it) both half closes
// end the session at once, as before, and nothing is acked.
func TestTCPTunnelEndAckOldAgentEndsAtOnce(t *testing.T) {
	a := NewTCPAgentStream()
	c, sid := endpointSession(t, a)
	a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0)
	if err := clientEnd(c, 10*time.Second); err != io.EOF {
		t.Fatalf("the client's read: %v, want EOF", err)
	}
	select {
	case f := <-a.SendCh:
		if f.GetSessionId() == dataplane.EndAckSID {
			t.Fatal("the relay acked on a stream without the end acknowledgement")
		}
	case <-time.After(50 * time.Millisecond):
	}
}

// TestTCPTunnelEndAckAgentAbortResetsClient: on a stream with the end
// acknowledgement the agent's whole close without its half close first
// (its target's read error, a stall reset, a refused open) is an abort:
// the client is reset and an in-process reader gets errTCPSessionAborted,
// never the clean end that would make a cut stream look whole
// (spec/README.md finding 15, abort-as-eof). The bytes sent before it are
// still delivered.
func TestTCPTunnelEndAckAgentAbortResetsClient(t *testing.T) {
	t.Run("endpoint", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		c, sid := endpointSessionHalf(t, a, false)
		a.Dispatch(&pb.TCPData{SessionId: sid, Data: []byte("head")}, 0)
		a.Dispatch(&pb.TCPData{SessionId: sid, Close: true}, 0)
		_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
		got := make([]byte, 4)
		if _, err := io.ReadFull(c, got); err != nil || string(got) != "head" {
			t.Fatalf("read %q, %v; want the bytes before the abort", got, err)
		}
		if err := clientEnd(c, 10*time.Second); err == nil || err == io.EOF {
			t.Fatalf("the client's read after the agent's abort: %v, want a reset", err)
		}
	})
	t.Run("in-process", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true
		sess := newTCPSession()
		sess.endAck = true
		a.addSession("s", sess)
		c := &inProcConn{agentStream: a, sessionID: "s", sess: sess, cleanupFn: func() { a.removeSession("s"); sess.markClosed() }}
		a.Dispatch(&pb.TCPData{SessionId: "s", Close: true}, 0)
		if _, err := c.Read(make([]byte, 1)); !errors.Is(err, errTCPSessionAborted) {
			t.Fatalf("read after the agent's abort: %v, want errTCPSessionAborted", err)
		}
	})
	t.Run("old-agent", func(t *testing.T) { // without the ack a whole close is also a normal end
		a := NewTCPAgentStream()
		c, sid := endpointSessionHalf(t, a, false)
		a.Dispatch(&pb.TCPData{SessionId: sid, Close: true}, 0)
		if err := clientEnd(c, 10*time.Second); err != io.EOF {
			t.Fatalf("the client's read: %v, want EOF", err)
		}
	})
}
