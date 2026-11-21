// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nfltr/link"
)

// A plain TCP visitor's end is delivered once the visitor's kernel
// acknowledged the FIN (spec/AgentLink.tla PassEnd, VisitorKernel,
// DoneTruth; spec/README.md finding 50, lane VISITOREND): passed to the
// relay's kernel is not delivered. Where the kernel cannot tell an ACK
// from an RST the end is not known delivered (errEndUnknown).

// errEndUnknown: the relay's kernel cannot tell whether the visitor's
// kernel acknowledged the FIN (macOS's CLOSED from LAST_ACK, an OS
// without TCP_INFO, a conn that is not TCP).
var errEndUnknown = errors.New("session: the kernel cannot tell the visitor's end delivered")

// finPollMax caps the wait between two looks at the kernel: no event
// tells an ACK, so the wait polls with backoff, bounded by endDoneIdle
// without progress.
const finPollMax = 50 * time.Millisecond

// visitorKernel is c's TCP socket, through the conns it wraps (NetConn:
// a PrefixConn, a counted conn), nil when c is not a TCP conn or its bytes
// are not the socket's (a TLS conn's).
func visitorKernel(c net.Conn) syscall.Conn {
	for {
		if _, ok := c.(*tls.Conn); ok {
			return nil
		}
		u, ok := c.(interface{ NetConn() net.Conn })
		if !ok {
			break
		}
		c = u.NetConn()
	}
	if _, ok := c.LocalAddr().(*net.TCPAddr); !ok {
		return nil
	}
	sc, _ := c.(syscall.Conn)
	return sc
}

// waitFinAcked waits until the kernel of k's peer acknowledged the FIN
// sent after written bytes: nil then; link.ErrReset after an RST or once
// k was closed; link.ErrEndAckIdle when nothing more was acknowledged for
// endDoneIdle (the far app stopped reading); errEndUnknown where the
// kernel cannot tell.
func waitFinAcked(k syscall.Conn, written int64) error {
	if k == nil {
		return errEndUnknown
	}
	rc, err := k.SyscallConn()
	if err != nil {
		return errEndUnknown
	}
	var tcp tcpEnd
	var last uint64
	since, delay := time.Now(), time.Millisecond
	for first := true; ; first = false {
		var done bool
		var progress uint64
		var ferr error
		if cerr := rc.Control(func(fd uintptr) { done, progress, ferr = tcp.finAcked(fd, written) }); cerr != nil {
			return link.ErrReset // closed here
		}
		if done || ferr != nil {
			return ferr
		}
		if first || progress != last {
			last, since = progress, time.Now()
		} else if time.Since(since) >= endDoneIdle {
			return link.ErrEndAckIdle
		}
		time.Sleep(delay)
		delay = min(2*delay, finPollMax)
	}
}

// finAckedConn is a visitor's conn (counted) that peerlink.Join passes
// the agent's end onto: it counts the bytes written, and its
// CloseWriteSync returns nil only once the visitor's kernel acknowledged
// the FIN; else it resets the visitor and Join aborts the agent's session.
type finAckedConn struct {
	net.Conn
	k syscall.Conn
	n atomic.Int64
}

// visitorEnd is c (what the relay copies through) syncing its end on
// visitor's kernel.
func visitorEnd(c, visitor net.Conn) net.Conn {
	return &finAckedConn{Conn: c, k: visitorKernel(visitor)}
}

func (c *finAckedConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	c.n.Add(int64(n))
	return n, err
}

func (c *finAckedConn) CloseWrite() error { return PassCloseWrite(c.Conn) }

func (c *finAckedConn) Reset() error {
	resetConn(c.Conn)
	return nil
}

func (c *finAckedConn) CloseWriteSync() error {
	if err := c.CloseWrite(); err != nil {
		return err
	}
	err := waitFinAcked(c.k, c.n.Load())
	if err != nil {
		resetConn(c.Conn) // before Join aborts the agent's session: the queued tail and FIN never go
	}
	return err
}
