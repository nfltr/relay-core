// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package session

import (
	"context"
	"net"
	"syscall"
	"testing"
	"time"
)

// A session whose target stops reading must not stall the agent's other
// sessions while its unread bytes fit in its own queue. The target's
// receive buffer is pinned small (set on the listening socket, so the
// accepted conn advertises a small window from the handshake on) so the
// kernel cannot absorb the burst on the session's behalf.
func TestTCPTunnelStuckTargetDoesNotStallOtherSessions(t *testing.T) {
	lc := net.ListenConfig{Control: func(_, _ string, c syscall.RawConn) error {
		var sockErr error
		if err := c.Control(func(fd uintptr) {
			sockErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
		}); err != nil {
			return err
		}
		return sockErr
	}}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := startTunnelHarness(t, serveTargets(t, ln), nil, "")

	stuck := h.Dial(t)
	defer func() { _ = stuck.Close() }()
	go func() {
		_, _ = stuck.Write(targetHeader('h', 0))
		_, _ = stuck.Write(make([]byte, 768<<10))
	}()
	time.Sleep(200 * time.Millisecond) // let the burst reach the agent

	done := make(chan error, 1)
	go func() { done <- echo(h, t, []byte("still flowing")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a stuck session stalled another session on the same agent stream")
	}
}
