// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay/proto/wire/dataplane"

	"github.com/nfltr/link"

	"github.com/nfltr/relay-core/core/directory"
)

// startDataPlane serves d's data plane on a loopback listener and returns
// its address and the listener gRPC would accept from.
func startDataPlane(t *testing.T, d *TCPDispatcher) (string, net.Listener) {
	t.Helper()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := d.DataPlaneListener(inner)
	t.Cleanup(func() { _ = ln.Close() })
	return inner.Addr().String(), ln
}

// TestDataConnectionRefused guards the retirement of agent data
// connections (nfltr docs/decisions/link-data-connections-retired-2026-11-20.md):
// no primary's header offers them (x-nfltr-tcp-data), and a link hello on
// the gRPC port, even with a live primary's bind token, gets link's
// refusal (reply status 1: the dialer's link.ErrRefused) and binds no
// auxiliary stream.
func TestDataConnectionRefused(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	addr, _ := startDataPlane(t, d)
	primary := &fake.EndableStream{End: make(chan struct{})}
	defer close(primary.End)
	go func() { _ = d.RegisterStream("a", primary, directory.Session{}, nil) }()
	waitStreamsOf(t, d, "a", 1)
	md := primary.SentHeader()
	if got := md.Get("x-nfltr-tcp-data"); len(got) != 0 {
		t.Fatalf("the primary's header offers data connections: %v", got)
	}
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = link.Client(ctx, nc, link.Config{Auth: dataplane.BindToken(md.Get(TCPBindHeader)[0])})
	if !errors.Is(err, link.ErrRefused) {
		t.Fatalf("a data connection with the primary's token: %v, want link.ErrRefused", err)
	}
	d.Mu.RLock()
	n := len(d.All["a"])
	d.Mu.RUnlock()
	if n != 1 {
		t.Fatalf("agent has %d streams, want its primary only", n)
	}
}

// TestDataPlaneListenerPassesGRPC guards the port's other users: a
// connection that does not open with the data plane's magic comes out of
// Accept with its first bytes intact.
func TestDataPlaneListenerPassesGRPC(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	addr, ln := startDataPlane(t, d)
	const preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	_, _ = nc.Write([]byte(preface))
	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	got := make([]byte, len(preface))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != preface {
		t.Fatalf("gRPC got %q, %v", got, err)
	}

}
