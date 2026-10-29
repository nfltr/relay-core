// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay/proto/pb"
)

// joinBufs holds the buffers WriteTo joins a backlog in; one is taken only
// for a backlog of several slices and returned right after its Write.
var joinBufs = sync.Pool{New: func() any { b := make([]byte, 0, tcpWriteBatch+tcpRelayBufSize); return &b }}

// ---------------------------------------------------------------------------
// inProcConn — net.Conn backed by TCPDispatcher in-process sessions
// ---------------------------------------------------------------------------

// inProcConn implements net.Conn for in-process TCPDispatcher sessions
// created by DialAgent. It reads the agent's bytes straight from the
// session's queue (no pipe or goroutine in between) and writes caller data
// into the agent stream's send channel.
type inProcConn struct {
	agentStream *tcpAgentStream
	sessionID   string
	sess        *tcpSession
	rest        []byte // unread tail of the last slice taken from the queue
	closed      atomic.Bool
	closeOnce   sync.Once
	cleanupFn   func()
}

// Read returns the agent's bytes in order, then io.EOF once the agent
// closed and the queued tail is read. After Close it returns io.EOF.
func (c *inProcConn) Read(b []byte) (int, error) {
	if len(c.rest) == 0 {
		data, ok := c.sess.recvQ.Pop()
		if !ok {
			return 0, c.sess.endErr()
		}
		c.rest = data
	}
	if c.closed.Load() {
		return 0, io.EOF
	}
	n := copy(b, c.rest)
	c.rest = c.rest[n:]
	return n, nil
}

// WriteTo writes the agent's bytes to w until the session ends (io.Copy
// uses it). A backlog goes out in one Write of up to tcpWriteBatch bytes,
// so serveConnectStream sends one HTTP/2 DATA frame for it instead of one
// per agent frame, each a round trip through the HTTP/2 server's writer.
func (c *inProcConn) WriteTo(w io.Writer) (int64, error) {
	var total int64
	write := func(p []byte) error {
		n, err := w.Write(p)
		total += int64(n)
		return err
	}
	if len(c.rest) > 0 {
		rest := c.rest
		c.rest = nil
		if err := write(rest); err != nil {
			return total, err
		}
	}
	var bufs [][]byte
	for {
		var ok bool
		if bufs, ok = c.sess.recvQ.PopBatch(bufs[:0], tcpWriteBatch); !ok || c.closed.Load() {
			if !ok {
				if err := c.sess.endErr(); err != io.EOF {
					return total, err
				}
			}
			return total, nil
		}
		var err error
		if len(bufs) == 1 {
			err = write(bufs[0])
		} else {
			jp := joinBufs.Get().(*[]byte)
			for _, b := range bufs {
				*jp = append(*jp, b...)
			}
			err = write(*jp)
			*jp = (*jp)[:0]
			joinBufs.Put(jp)
		}
		clear(bufs)
		if err != nil {
			return total, err
		}
	}
}

// Write sends data to the agent.  It copies b to prevent data races with the
// caller reusing the slice before the send channel consumes it.
func (c *inProcConn) Write(b []byte) (int, error) {
	if c.writeEnded() {
		return 0, io.ErrClosedPipe
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	c.agentStream.send(&pb.TCPData{SessionId: c.sessionID, Data: cp})
	return len(b), nil
}

// WriteOwned sends b to the agent without a copy: the caller gives b up
// (dataplane.OwnedWriter: a tcp-connect data connection's session).
func (c *inProcConn) WriteOwned(b []byte) error {
	if c.writeEnded() {
		return io.ErrClosedPipe
	}
	c.agentStream.send(&pb.TCPData{SessionId: c.sessionID, Data: b})
	return nil
}

// Reset ends the session as Close does: the frame streams carry no reset.
func (c *inProcConn) Reset() error { return c.Close() }

// Close sends a TCP FIN to the agent and releases all session resources.
// writeEnded reports whether the session takes no more bytes: it ended,
// or this side half-closed it.
func (c *inProcConn) writeEnded() bool {
	select {
	case <-c.sess.closed:
		return true
	default:
		return c.sess.selfHalf.Load()
	}
}

// CloseWrite half-closes the session: the agent's target reads EOF after
// what was written; Read still returns the agent's bytes.
func (c *inProcConn) CloseWrite() error {
	c.agentStream.closeWrite(c.sessionID, c.sess)
	return nil
}

func (c *inProcConn) Close() error {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if !c.sess.selfHalf.Load() || !c.sess.peerHalf.Load() { // both half closes ended it already
			c.agentStream.send(&pb.TCPData{SessionId: c.sessionID, Close: true})
		}
		c.cleanupFn()
	})
	return nil
}

func (c *inProcConn) LocalAddr() net.Addr  { return inProcAddr{"local"} }
func (c *inProcConn) RemoteAddr() net.Addr { return inProcAddr{"remote"} }

// Deadline methods are no-ops; context cancellation drives session lifetime.
func (c *inProcConn) SetDeadline(t time.Time) error      { return nil }
func (c *inProcConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *inProcConn) SetWriteDeadline(t time.Time) error { return nil }

// inProcAddr is a placeholder net.Addr for in-process connections.
type inProcAddr struct{ addr string }

func (a inProcAddr) Network() string { return "inproc" }
func (a inProcAddr) String() string  { return a.addr }
