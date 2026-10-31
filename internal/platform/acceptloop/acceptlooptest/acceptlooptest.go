// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package acceptlooptest is a listener whose Accept fails the way a process
// at its file descriptor limit sees it, for accept loops' tests.
package acceptlooptest

import (
	"net"
	"os"
	"sync"
	"syscall"
)

// EMFILEListener fails its first Accept with EMFILE, closes Again at the
// next Accept, which then blocks until Close and returns net.ErrClosed.
type EMFILEListener struct {
	Again  chan struct{}
	mu     sync.Mutex
	calls  int
	closed chan struct{}
	once   sync.Once
}

// New returns an EMFILEListener.
func New() *EMFILEListener {
	return &EMFILEListener{Again: make(chan struct{}), closed: make(chan struct{})}
}

func (l *EMFILEListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	l.calls++
	n := l.calls
	l.mu.Unlock()
	if n == 1 {
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: os.NewSyscallError("accept", syscall.EMFILE)}
	}
	if n == 2 {
		close(l.Again)
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *EMFILEListener) Close() error   { l.once.Do(func() { close(l.closed) }); return nil }
func (l *EMFILEListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)} }
