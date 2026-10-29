// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"log/slog"
	"net"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"

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

// dialData opens a data connection and binds it with token, as an agent
// does: its net.Conn (for deadlines) and its frames.
func dialData(t *testing.T, addr, token string) (net.Conn, *dataplane.Conn, error) {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	dc, err := dataplane.Dial(ctx, nc, token, link.Config{Window: 1 << 20})
	return nc, dc, err
}

// testToken is a bind token no primary issued.
var testToken = strings.Repeat("0", 32)

// TestDataConnsBindToPrimary guards the data plane's binding: the relay
// offers it in the primary's header; a connection with the primary's token
// joins the agent's rotation as an auxiliary stream (no agent of its own);
// an unknown token, another version, or one past tcpAgentStreams-1 are
// refused; and they all close when the primary ends.
func TestDataConnsBindToPrimary(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	addr, _ := startDataPlane(t, d)
	primary := &fake.EndableStream{End: make(chan struct{})}
	primaryDone := make(chan struct{})
	go func() { _ = d.RegisterStream("a", primary, directory.Session{}, nil); close(primaryDone) }()
	waitStreamsOf(t, d, "a", 1)
	md := primary.SentHeader()
	if got := md.Get(dataplane.Header); !slices.Equal(got, dataplane.Versions) || !slices.Equal(got, []string{"2"}) {
		t.Fatalf("primary header %v, want %s: 2", md, dataplane.Header)
	}
	token := md.Get(TCPBindHeader)[0]

	if _, _, err := dialData(t, addr, testToken); !errors.Is(err, dataplane.ErrRefused) {
		t.Fatalf("unknown token: %v, want refused", err)
	}
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = nc.Write([]byte(dataplane.Magic + "\x03" + token + "\x00\x10\x00\x00"))
	_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	b := make([]byte, 2)
	if _, err := io.ReadFull(nc, b); err != nil || !bytes.Equal(b, []byte{linkStatusVersion, link.MaxVersion}) {
		t.Fatalf("version 3 answered %x, %v; want the versions spoken", b, err)
	}
	_ = nc.Close()
	// A version-1 hello (an agent before nfltr v2.0.0) gets the version
	// reply, which it reads as a refusal.
	if nc, err = net.Dial("tcp", addr); err != nil {
		t.Fatal(err)
	}
	_, _ = nc.Write([]byte(dataplane.Magic + "\x01" + token + "\x00\x10\x00\x00"))
	_ = nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(nc, b); err != nil || !bytes.Equal(b, []byte{linkStatusVersion, link.MaxVersion}) {
		t.Fatalf("version 1 answered %x, %v; want the versions spoken", b, err)
	}
	_ = nc.Close()

	conns := make([]net.Conn, tcpAgentStreams-1)
	dcs := make([]*dataplane.Conn, len(conns))
	for i := range conns {
		if conns[i], dcs[i], err = dialData(t, addr, token); err != nil {
			t.Fatalf("data connection %d: %v", i, err)
		}
	}
	waitStreamsOf(t, d, "a", tcpAgentStreams)
	d.Mu.RLock()
	for _, as := range d.All["a"][1:] {
		if as.DC == nil || as.primary == nil {
			t.Error("a data connection is not an auxiliary stream of the primary")
		}
	}
	d.Mu.RUnlock()
	for range 2 * len(conns) { // new sessions take the data connections
		if as := d.agentStream("a"); as.DC == nil {
			t.Fatal("a new session went to the gRPC stream while data connections were open")
		}
	}
	if _, _, err := dialData(t, addr, token); !errors.Is(err, dataplane.ErrRefused) {
		t.Fatalf("binding past %d auxiliary streams: %v, want refused", tcpAgentStreams-1, err)
	}
	if ids := d.ConnectedAgentIDs(); len(ids) != 1 {
		t.Fatalf("connected agents %v, want one", ids)
	}

	close(primary.End) // the primary ends: its data connections close
	<-primaryDone
	for i, c := range conns {
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		for r := dcs[i]; ; {
			if _, err := r.Recv(); err != nil {
				if ne := net.Error(nil); errors.As(err, &ne) && ne.Timeout() {
					t.Fatal("a data connection outlived its primary")
				}
				break
			}
		}
	}
	if _, _, err := dialData(t, addr, token); !errors.Is(err, dataplane.ErrRefused) {
		t.Fatalf("the ended primary's token: %v, want refused", err)
	}
	waitStreamsOf(t, d, "a", 0)
}

// TestDataPlaneListenerPassesGRPC guards the port's other users: a
// connection that does not open with the data plane's magic comes out of
// Accept with its first bytes intact, and a relay without the listener
// does not offer the data plane.
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

	plain := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	primary := &fake.EndableStream{End: make(chan struct{})}
	defer close(primary.End)
	go func() { _ = plain.RegisterStream("a", primary, directory.Session{}, nil) }()
	waitStreamsOf(t, plain, "a", 1)
	if got := primary.SentHeader().Get(dataplane.Header); len(got) != 0 {
		t.Fatalf("a relay without the data plane offered it: %v", got)
	}
}

// TestDataConnCarriesFrames moves frames both ways over a bound data
// connection: an open frame the relay queues reaches the agent, and data
// the agent sends reaches the session.
func TestDataConnCarriesFrames(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	addr, _ := startDataPlane(t, d)
	primary := &fake.EndableStream{End: make(chan struct{})}
	defer close(primary.End)
	go func() { _ = d.RegisterStream("a", primary, directory.Session{}, nil) }()
	waitStreamsOf(t, d, "a", 1)
	_, agentSide, err := dialData(t, addr, primary.SentHeader().Get(TCPBindHeader)[0])
	if err != nil {
		t.Fatal(err)
	}
	waitStreamsOf(t, d, "a", 2)
	d.Mu.RLock()
	as := d.All["a"][1]
	d.Mu.RUnlock()
	sess := newTCPSession()
	if !as.addSession("s1", sess) {
		t.Fatal("data stream refused a session")
	}
	as.send(&pb.TCPData{SessionId: "s1", Open: true, TargetAddr: "t:1"})

	f, err := agentSide.Recv()
	if err != nil || !f.GetOpen() || f.GetTargetAddr() != "t:1" {
		t.Fatalf("agent got %v, %v; want the open frame", f, err)
	}
	if err := agentSide.Send(&pb.TCPData{SessionId: "s1", Data: []byte("pong")}); err != nil {
		t.Fatal(err)
	}
	_ = agentSide.Send(&pb.TCPData{SessionId: "s1", Close: true})
	_ = agentSide.Flush()
	b, ok := sess.recvQ.Pop()
	if !ok || string(b) != "pong" {
		t.Fatalf("session got %q, %v", b, ok)
	}
	select {
	case <-sess.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("close frame did not reach the session")
	}
}

// TestDataConnSlowSessionDoesNotStallOthers guards per-session credit on a
// data connection: an agent sends a session whose client does not read at
// most the relay's window, the relay's receive loop never waits on that
// session's full queue, so another session on the same connection still
// gets its data, and once the slow client reads the rest follows.
func TestDataConnSlowSessionDoesNotStallOthers(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	addr, _ := startDataPlane(t, d)
	primary := &fake.EndableStream{End: make(chan struct{})}
	defer close(primary.End)
	go func() { _ = d.RegisterStream("a", primary, directory.Session{}, nil) }()
	waitStreamsOf(t, d, "a", 1)
	_, agentSide, err := dialData(t, addr, primary.SentHeader().Get(TCPBindHeader)[0])
	if err != nil {
		t.Fatal(err)
	}
	waitStreamsOf(t, d, "a", 2)
	d.Mu.RLock()
	as := d.All["a"][1]
	d.Mu.RUnlock()
	slow, fast := newTCPSession(), newTCPSession()
	if d.attach("a", as, "slow", slow) != as || d.attach("a", as, "fast", fast) != as {
		t.Fatal("the data stream did not take both sessions")
	}

	agentSide.Credit().Open("slow")
	agentSide.Credit().Open("fast")
	go func() { // applies the relay's window frames
		for {
			if _, err := agentSide.Recv(); err != nil {
				return
			}
		}
	}()
	chunk := make([]byte, dataplane.MaxChunk)
	var sent atomic.Int64
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		for range 3 * TCPDataWindow / len(chunk) {
			if !agentSide.Credit().Acquire("slow", len(chunk)) {
				return
			}
			_ = agentSide.Send(&pb.TCPData{SessionId: "slow", Data: chunk})
			_ = agentSide.Flush()
			sent.Add(int64(len(chunk)))
		}
	}()
	waitUntil(t, func() bool { return sent.Load() == TCPDataWindow }, func() string {
		return fmt.Sprintf("agent sent %d bytes of a session nobody reads, want its window %d", sent.Load(), TCPDataWindow)
	})

	if !agentSide.Credit().Acquire("fast", 4) {
		t.Fatal("no credit for another session")
	}
	_ = agentSide.Send(&pb.TCPData{SessionId: "fast", Data: []byte("fast")})
	_ = agentSide.Flush()
	got := make(chan []byte, 1)
	go func() { b, _ := fast.recvQ.Pop(); got <- b }()
	select {
	case b := <-got:
		if string(b) != "fast" {
			t.Fatalf("fast session got %q", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a session nobody reads stalled another on the same data connection")
	}

	var read int
	for read < 3*TCPDataWindow {
		b, ok := slow.recvQ.Pop()
		if !ok {
			t.Fatal("slow session queue closed")
		}
		read += len(b)
	}
	<-slowDone
}

// linkStatusVersion is the handshake's version reply status (link's WIRE.md).
const linkStatusVersion = 2
