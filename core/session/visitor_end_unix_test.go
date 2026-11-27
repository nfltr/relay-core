// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package session

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// farRcvBuf is the far app's receive buffer when it stops reading: set
// before the handshake, so its window stays that small.
const farRcvBuf = 4096

// tcpPair is a relay-side conn (near) and the far app's (far) on
// loopback; small: far's receive buffer is farRcvBuf, near's send buffer
// large enough to take what the far one does not.
func tcpPair(t *testing.T, small bool) (near, far *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	var d net.Dialer
	if small {
		d.Control = func(_, _ string, rc syscall.RawConn) error {
			return rc.Control(func(fd uintptr) { _ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, farRcvBuf) })
		}
	}
	fc, err := d.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	nc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	near, far = nc.(*net.TCPConn), fc.(*net.TCPConn)
	if small {
		_ = far.SetReadBuffer(farRcvBuf) // again: macOS raises a loopback socket's at connect
		_ = near.SetWriteBuffer(stuckSndBuf)
	}
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	return near, far
}

// stuckTail is more than the far app's window takes, and less than
// near's send buffer (stuckSndBuf) holds besides: near's writes return
// while the FIN waits behind them, the far app reading nothing. macOS
// grows a loopback receive buffer past SO_RCVBUF, up to
// net.inet.tcp.autorcvbufmax (4 MiB); Linux keeps it (SOCK_RCVBUF_LOCK)
// and caps a send buffer at net.core.wmem_max (doubled).
var stuckTail, stuckSndBuf = func() ([]byte, int) {
	if runtime.GOOS == "darwin" {
		return bytes.Repeat([]byte("x"), 6<<20), 6 << 20
	}
	return bytes.Repeat([]byte("x"), 128<<10), 256 << 10
}()

// TestWaitFinAcked: the visitor's end is delivered once its kernel
// acknowledged the FIN: a far app that reads gets it (nil); one that
// stops reading never acknowledges it (link.ErrEndAckIdle, never nil);
// one that resets with bytes unread is link.ErrReset; a conn that is not
// TCP cannot tell (errEndUnknown). Crossing ends (the far app's FIN
// first: the relay's from LAST_ACK) are delivered on Linux and unknown
// on macOS (spec/AgentLink.tla VisitorKernel, lane VISITOREND).
func TestWaitFinAcked(t *testing.T) {
	old := endDoneIdle
	endDoneIdle = 300 * time.Millisecond
	t.Cleanup(func() { endDoneIdle = old })
	wait := func(t *testing.T, near *net.TCPConn, n int) error {
		t.Helper()
		if err := near.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		res := make(chan error, 1)
		go func() { res <- waitFinAcked(visitorKernel(near), int64(n)) }()
		select {
		case err := <-res:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("waitFinAcked did not return")
			return nil
		}
	}
	t.Run("reads", func(t *testing.T) {
		near, far := tcpPair(t, false)
		go func() { _, _ = io.Copy(io.Discard, far) }()
		n, _ := near.Write([]byte("tail"))
		if err := wait(t, near, n); err != nil {
			t.Fatalf("the far app read every byte and the end: %v, want nil", err)
		}
	})
	t.Run("stops reading", func(t *testing.T) {
		near, _ := tcpPair(t, true)
		n, err := near.Write(stuckTail)
		if err != nil {
			t.Fatal(err)
		}
		if err := wait(t, near, n); !errors.Is(err, link.ErrEndAckIdle) {
			t.Fatalf("the far app read nothing: %v, want link.ErrEndAckIdle", err)
		}
	})
	t.Run("resets", func(t *testing.T) {
		near, far := tcpPair(t, true)
		n, err := near.Write(stuckTail)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			time.Sleep(50 * time.Millisecond) // the FIN is queued, unacknowledged
			_ = far.SetLinger(0)
			_ = far.Close()
		}()
		if err := wait(t, near, n); !errors.Is(err, link.ErrReset) {
			t.Fatalf("the far app reset with bytes unread: %v, want link.ErrReset", err)
		}
	})
	t.Run("crossing", func(t *testing.T) {
		near, far := tcpPair(t, false)
		_ = far.CloseWrite()
		if _, err := near.Read(make([]byte, 1)); err != io.EOF {
			t.Fatalf("near read %v, want the far app's EOF", err)
		}
		go func() { _, _ = io.Copy(io.Discard, far) }()
		n, _ := near.Write([]byte("tail"))
		err := wait(t, near, n)
		if runtime.GOOS == "darwin" {
			if !errors.Is(err, errEndUnknown) && !errors.Is(err, link.ErrReset) {
				t.Fatalf("macOS from LAST_ACK: %v, want not delivered", err)
			}
			return
		}
		if err != nil {
			t.Fatalf("Linux from LAST_ACK, the far app read it: %v, want nil", err)
		}
	})
	t.Run("join", func(t *testing.T) { // the agent link: Join passes the end through finAckedConn
		for _, reads := range []bool{true, false} {
			near, far := tcpPair(t, !reads)
			if reads {
				go func() { _, _ = io.Copy(io.Discard, far) }()
			}
			c := visitorEnd(near, near).(*finAckedConn)
			tail := stuckTail
			if reads {
				tail = []byte("tail")
			}
			if _, err := c.Write(tail); err != nil {
				t.Fatal(err)
			}
			if err := c.CloseWriteSync(); (err == nil) != reads {
				t.Fatalf("CloseWriteSync, the far app reading %v: %v", reads, err)
			}
		}
	})
	t.Run("counted sni visitor", func(t *testing.T) { // frontend/sni: CountConn(PrefixConn{conn})
		near, far := tcpPair(t, false)
		go func() { _, _ = io.Copy(io.Discard, far) }()
		v := metrics.CountConn(&PrefixConn{Conn: near}, metrics.PathSNI, nil, time.Now(), false, true)
		if visitorKernel(v) == nil {
			t.Fatal("a counted, prefixed visitor's TCP socket is not reached")
		}
		if visitorKernel(tls.Client(near, &tls.Config{})) != nil { // #nosec G402 -- never handshakes
			t.Fatal("a TLS conn's bytes are not its socket's: want no kernel")
		}
	})
	t.Run("not tcp", func(t *testing.T) {
		p, q := net.Pipe()
		defer p.Close() //nolint:errcheck
		defer q.Close() //nolint:errcheck
		if err := waitFinAcked(visitorKernel(p), 0); !errors.Is(err, errEndUnknown) {
			t.Fatalf("a pipe: %v, want errEndUnknown", err)
		}
	})
}

// visitorSession is a session of a (with the delivery report) whose
// visitor is far, its relay-side conn near (tcpPair), served by
// handleLocalConn; its id.
func visitorSession(t *testing.T, a *tcpAgentStream, small bool) (far *net.TCPConn, sid string) {
	t.Helper()
	near, far := tcpPair(t, small)
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.handleLocalConn(near, TCPEndpoint{AgentID: "agent"}, a)
		_ = near.Close()
	}()
	t.Cleanup(func() { a.CloseAll(); <-done })
	return far, nextFrame(t, a, "open", (*pb.TCPData).GetOpen).GetSessionId()
}

// TestTCPTunnelVisitorEndDoneOnceKernelAcked: on the gRPC streams the
// relay reports the agent's half close delivered (EndDoneFrame) only once
// the visitor's kernel acknowledged the FIN; a visitor that stops reading
// gets no report, and the session is aborted toward the agent
// (TCPData.abort) and cut at the visitor (spec/AgentLink.tla DoneTruth,
// VisitorAbort; mutant report-on-write; lane VISITOREND).
func TestTCPTunnelVisitorEndDoneOnceKernelAcked(t *testing.T) {
	old := endDoneIdle
	endDoneIdle = 300 * time.Millisecond
	t.Cleanup(func() { endDoneIdle = old })
	t.Run("reads", func(t *testing.T) {
		a := endDoneStream()
		far, sid := visitorSession(t, a, false)
		a.Dispatch(&pb.TCPData{SessionId: sid, Data: []byte("tail")}, 0)
		a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0)
		_ = far.SetReadDeadline(time.Now().Add(10 * time.Second))
		if b, err := io.ReadAll(far); err != nil || string(b) != "tail" {
			t.Fatalf("the visitor read %q, %v; want tail, then EOF", b, err)
		}
		nextFrame(t, a, "delivery report", func(f *pb.TCPData) bool {
			return f.GetSessionId() == dataplane.EndDoneSID && string(f.GetData()) == sid
		})
	})
	t.Run("stops reading", func(t *testing.T) {
		a := endDoneStream()
		far, sid := visitorSession(t, a, true)
		for off := 0; off < len(stuckTail); off += 16 << 10 {
			a.Dispatch(&pb.TCPData{SessionId: sid, Data: stuckTail[off : off+16<<10]}, 10*time.Second) // waits for room: no stall reset
		}
		a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0)
		halfAt := time.Now()
		deadline := time.After(10 * time.Second)
		for aborted := false; !aborted; {
			select {
			case f := <-a.SendCh:
				if f.GetSessionId() == dataplane.EndDoneSID {
					t.Fatal("the relay reported the end delivered to a visitor that read nothing")
				}
				aborted = f.GetSessionId() == sid && f.GetClose() && f.GetAbort()
			case <-deadline:
				t.Fatal("the relay sent no abort for an end not known delivered")
			}
		}
		if d := time.Since(halfAt); d < endDoneIdle {
			t.Fatalf("aborted %v after the half close, before the idle bound %v: not the kernel's wait", d, endDoneIdle)
		}
		if err := clientEnd(far, 10*time.Second); err == nil || err == io.EOF {
			t.Fatalf("the visitor's read after the abort: %v, want a reset", err)
		}
	})
}

// slowCut is a visitor's conn whose Close wakes its reader (the pump)
// first and closes the socket a moment later, as net.Conn.Close does
// under load: the woken read fails before close(2) has reset anything.
type slowCut struct {
	*net.TCPConn
	cut atomic.Bool
}

func (c *slowCut) Close() error {
	_ = c.SetReadDeadline(time.Unix(1, 0)) // the pump's read fails now
	time.Sleep(100 * time.Millisecond)
	err := c.TCPConn.Close()
	c.cut.Store(true)
	return err
}

// TestTCPTunnelVisitorAbortAfterReset: the abort of an end not known
// delivered reaches the agent only once the visitor's reset is done
// (spec/AgentLink.tla VisitorAbort: the visitor first). Before, the
// pump's read, failing as the reset's Close began, sent the abort while
// the visitor's kernel could still send the queued tail and FIN: a
// visitor reading again then got a clean end (box
// 20261122T011741Z-relay-core-0d30b042b80e.log, lane LANDREDS).
func TestTCPTunnelVisitorAbortAfterReset(t *testing.T) {
	old := endDoneIdle
	endDoneIdle = 300 * time.Millisecond
	t.Cleanup(func() { endDoneIdle = old })
	a := endDoneStream()
	near, _ := tcpPair(t, true)
	v := &slowCut{TCPConn: near}
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.handleLocalConn(v, TCPEndpoint{AgentID: "agent"}, a)
	}()
	t.Cleanup(func() { a.CloseAll(); <-done })
	sid := nextFrame(t, a, "open", (*pb.TCPData).GetOpen).GetSessionId()
	for off := 0; off < len(stuckTail); off += 16 << 10 {
		a.Dispatch(&pb.TCPData{SessionId: sid, Data: stuckTail[off : off+16<<10]}, 10*time.Second)
	}
	a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0)
	nextFrame(t, a, "abort", func(f *pb.TCPData) bool { return f.GetSessionId() == sid && f.GetAbort() })
	if !v.cut.Load() {
		t.Fatal("the agent got the abort before the visitor's reset was done")
	}
}

// TestJoinVisitorReadErrorAfterReset: on the agent link the same holds
// for Join's read of the visitor: finAckedConn's read error from the
// reset that CloseWriteSync does comes back only once the reset is done,
// so Join aborts nothing before it.
func TestJoinVisitorReadErrorAfterReset(t *testing.T) {
	old := endDoneIdle
	endDoneIdle = 300 * time.Millisecond
	t.Cleanup(func() { endDoneIdle = old })
	near, _ := tcpPair(t, true)
	v := &slowCut{TCPConn: near}
	c := visitorEnd(v, v).(*finAckedConn)
	cutFirst := make(chan bool, 1)
	go func() {
		_, err := c.Read(make([]byte, 1))
		cutFirst <- err != nil && v.cut.Load()
	}()
	if _, err := c.Write(stuckTail); err != nil {
		t.Fatal(err)
	}
	if err := c.CloseWriteSync(); err == nil {
		t.Fatal("CloseWriteSync to a visitor that reads nothing: nil")
	}
	if !<-cutFirst {
		t.Fatal("Join's read of the visitor failed before its reset was done")
	}
}
