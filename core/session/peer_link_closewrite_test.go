// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"net"
	"testing"
)

// TestAgentLinkConnPassesCloseWrite: the agent link's conn wrapper passes
// CloseWrite on, which github.com/nfltr/link's graceful close (a drain)
// needs (the replica link's twin is in core/replicas).
func TestAgentLinkConnPassesCloseWrite(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	peer, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	nc, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	c := net.Conn(&agentLinkConn{Conn: &PrefixConn{Conn: nc}, l: &agentLink{}})
	defer c.Close()
	cw, ok := c.(interface{ CloseWrite() error })
	if !ok {
		t.Fatalf("%T hides CloseWrite", c)
	}
	if _, err := c.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := cw.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if b, err := io.ReadAll(peer); err != nil || string(b) != "tail" {
		t.Fatalf("peer read %q, %v; want the tail, then io.EOF", b, err)
	}
	if _, err := peer.Write([]byte("back")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 4)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "back" {
		t.Fatalf("read after CloseWrite: %q %v", b, err)
	}
}
