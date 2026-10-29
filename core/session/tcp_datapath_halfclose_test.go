// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

// TestTCPTunnelHalfClose: a half close passes through the tunnel both
// ways. A client that half-closes (shutdown(SHUT_WR): nc -N, a request
// then EOF) still gets the reply the target sends after its EOF, and a
// target that half-closes first still gets the client's later bytes, on
// every way a session reaches the agent (the agent link over TCP and
// QUIC, data connections, gRPC streams) and from both entries: a TCP
// endpoint's visitor, and an in-process session (DialAgent: tcp-connect,
// the SSH proxy).
func TestTCPTunnelHalfClose(t *testing.T) {
	for _, tc := range []struct {
		name string
		ag   harnessAgent
		link string // the agent link's transport, "" for frame streams
	}{
		{"link-tcp", agentCurrent, "tcp"},
		{"link-quic", agentQUIC, "quic"},
		{"data-conns", agentDataConns, ""},
		{"aux-grpc", agentAuxOnly, ""},
		{"one-grpc", agentOneConn, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tgt := startHalfCloseTarget(t)
			h := startTunnelHarnessWith(t, tgt.addr, nil, "", 0, relayCurrent, tc.ag)
			entries := []struct {
				name string
				dial func() (net.Conn, error)
			}{
				{"endpoint", func() (net.Conn, error) { return h.Dial(t), nil }},
				{"in-process", func() (net.Conn, error) { return h.D.DialAgent(t.Context(), harnessAgentID, tgt.addr) }},
			}
			if tc.link != "" {
				c, _ := entries[0].dial()
				if err := clientHalfCloses(c, 1); err != nil {
					t.Fatal(err)
				}
				waitLinkTransport(t, h, tc.link)
			}
			for _, e := range entries {
				t.Run(e.name, func(t *testing.T) {
					if err := runConcurrent(4, func() error {
						c, err := e.dial()
						if err != nil {
							return err
						}
						return clientHalfCloses(c, 64<<10)
					}); err != nil {
						t.Fatalf("client half close: %v", err)
					}
					if err := runConcurrent(4, func() error {
						c, err := e.dial()
						if err != nil {
							return err
						}
						return targetHalfCloses(c, tgt, 64<<10)
					}); err != nil {
						t.Fatalf("target half close: %v", err)
					}
				})
			}
		})
	}
}

// halfCloseTarget is a target whose connections start with a mode byte:
//
//	'r' reply at EOF: read to EOF, then write "got <n>" and close (a
//	    request ended by a half close: nc -N, HTTP/1.0).
//	's' speak first: write "hello", half-close, read to EOF and report the
//	    count on late, then close.
type halfCloseTarget struct {
	addr string
	late chan int64
}

func startHalfCloseTarget(tb testing.TB) *halfCloseTarget {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = ln.Close() })
	t := &halfCloseTarget{addr: ln.Addr().String(), late: make(chan int64, 64)}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close() //nolint:errcheck
				mode := make([]byte, 1)
				if _, err := io.ReadFull(c, mode); err != nil {
					return
				}
				switch mode[0] {
				case 'r':
					n, _ := io.Copy(io.Discard, c)
					_, _ = fmt.Fprintf(c, "got %d", n)
				case 's':
					_, _ = c.Write([]byte("hello"))
					_ = c.(*net.TCPConn).CloseWrite()
					n, _ := io.Copy(io.Discard, c)
					t.late <- n
				}
			}()
		}
	}()
	return t
}

// halfClose half-closes c, failing when c cannot.
func halfClose(c net.Conn) error {
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		return fmt.Errorf("%T has no half close", c)
	}
	return cw.CloseWrite()
}

// clientHalfCloses sends a request, half-closes and wants the reply the
// target sends only after its EOF.
func clientHalfCloses(c net.Conn, n int) error {
	defer c.Close() //nolint:errcheck
	stop := time.AfterFunc(10*time.Second, func() { _ = c.Close() })
	defer stop.Stop()
	if _, err := c.Write(append([]byte{'r'}, bytes.Repeat([]byte("x"), n)...)); err != nil {
		return err
	}
	if err := halfClose(c); err != nil {
		return err
	}
	got, err := io.ReadAll(c)
	if want := fmt.Sprintf("got %d", n); string(got) != want {
		return fmt.Errorf("after a half close read %q (%v), want %q", got, err, want)
	}
	return nil
}

// targetHalfCloses reads the target's greeting to its EOF, then sends n
// bytes the target counts after its own half close.
func targetHalfCloses(c net.Conn, tgt *halfCloseTarget, n int) error {
	defer c.Close() //nolint:errcheck
	stop := time.AfterFunc(10*time.Second, func() { _ = c.Close() })
	defer stop.Stop()
	if _, err := c.Write([]byte{'s'}); err != nil {
		return err
	}
	if got, err := io.ReadAll(c); string(got) != "hello" {
		return fmt.Errorf("read %q (%v), want the greeting then EOF", got, err)
	}
	if _, err := c.Write(bytes.Repeat([]byte("y"), n)); err != nil {
		return fmt.Errorf("write after the target's half close: %w", err)
	}
	if err := halfClose(c); err != nil {
		return err
	}
	select {
	case got := <-tgt.late:
		if got != int64(n) {
			return fmt.Errorf("the target read %d bytes after its half close, want %d", got, n)
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("the target never read to EOF after its half close")
	}
	return nil
}
