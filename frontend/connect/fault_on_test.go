// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relayfault

package connect

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/nfltr/link/peerlink"
)

type resetConn struct {
	net.Conn
	reset bool
}

func (c *resetConn) CloseWrite() error { return nil }
func (c *resetConn) Reset() error      { c.reset = true; return c.Conn.Close() }

func faultPair(t *testing.T) (*resetConn, net.Conn) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close(); _ = b.Close() })
	return &resetConn{Conn: a}, b
}

func TestFaultWithholdsTLSFirstByte(t *testing.T) {
	file := filepath.Join(t.TempDir(), "withhold")
	t.Setenv("RELAY_FAULT_WITHHOLD_FILE", file)

	s, _ := faultPair(t)
	if got := faultSession("nfltr-direct", s); got != peerlink.Session(s) {
		t.Fatal("no file: the session must pass unwrapped")
	}
	if err := os.WriteFile(file, []byte("nfltr-direct\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := faultSession("blob", s); got != peerlink.Session(s) {
		t.Fatal("another service must pass unwrapped")
	}

	// a control exchange: its first byte opens a TLS record
	s, _ = faultPair(t)
	if _, err := faultSession("nfltr-direct", s).Write([]byte{tlsHandshake, 3, 1}); err == nil || !s.reset {
		t.Fatalf("TLS first byte: err %v, reset %v; want an error and a reset", err, s.reset)
	}

	// a relayed re-attach: its first byte is a path-layer frame
	s, peer := faultPair(t)
	w := faultSession("nfltr-direct", s)
	go func() { buf := make([]byte, 8); _, _ = peer.Read(buf); _, _ = peer.Read(buf) }()
	for _, p := range [][]byte{{0x01, 2}, {tlsHandshake}} { // only the first byte counts
		if _, err := w.Write(p); err != nil || s.reset {
			t.Fatalf("write %x: err %v, reset %v; want it passed", p, err, s.reset)
		}
	}
}
