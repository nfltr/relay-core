// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"net"
	"sync"
)

// SessionListener hands link sessions to an http.Server.
type SessionListener struct {
	addr   net.Addr
	ch     chan net.Conn
	done   chan struct{}
	closed sync.Once
}

// NewSessionListener returns a listener reporting addr as its address.
func NewSessionListener(addr net.Addr) *SessionListener {
	return &SessionListener{addr: addr, ch: make(chan net.Conn), done: make(chan struct{})}
}

// Push hands c to the next Accept, waiting for one; after Close it resets c.
func (l *SessionListener) Push(c net.Conn) {
	select {
	case l.ch <- c:
	case <-l.done:
		resetConn(c)
	}
}

// Accept returns the next pushed connection, or net.ErrClosed after Close.
func (l *SessionListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Close ends Accept and Push; it is safe to call more than once.
func (l *SessionListener) Close() error {
	l.closed.Do(func() { close(l.done) })
	return nil
}

// Addr returns the address NewSessionListener was given.
func (l *SessionListener) Addr() net.Addr { return l.addr }

// Done is closed by Close.
func (l *SessionListener) Done() <-chan struct{} { return l.done }
