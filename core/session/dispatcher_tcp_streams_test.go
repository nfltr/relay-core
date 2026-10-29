// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nfltr/relay-core/core/directory"
)

// TestAgentStreamsRegistry guards the per-agent stream set: the relay asks
// for tcpAgentStreams streams; a primary stream that registers after
// another of the agent (its reconnect, while the relay has not yet seen
// the old connection die) takes every new session, the older one none (it
// may be dead: a session there would hang or end "connection lost"); the
// newest ending leaves the agent reachable over the older; only the last
// one ending removes the agent.
func TestAgentStreamsRegistry(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	register := func() (*fake.EndableStream, chan struct{}) {
		s := &fake.EndableStream{End: make(chan struct{})}
		done := make(chan struct{})
		go func() { _ = d.RegisterStream("a", s, directory.Session{}, nil); close(done) }()
		return s, done
	}
	waitStreams := func(want int) {
		t.Helper()
		for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
			d.Mu.RLock()
			n := len(d.All["a"])
			d.Mu.RUnlock()
			if n == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("agent has %d streams, want %d", n, want)
			}
		}
	}

	first, firstDone := register()
	waitStreams(1)
	if got := first.SentHeader().Get(tcpAgentStreamsHeader); len(got) != 1 || got[0] != strconv.Itoa(tcpAgentStreams) {
		t.Fatalf("stream header %v, want %s: %d", first.SentHeader(), tcpAgentStreamsHeader, tcpAgentStreams)
	}
	d.Mu.RLock()
	older := d.Streams["a"]
	d.Mu.RUnlock()
	second, secondDone := register()
	waitStreams(2)
	d.Mu.RLock()
	newest := d.Streams["a"]
	d.Mu.RUnlock()
	for range 8 {
		if as := d.agentStream("a"); as != newest {
			t.Fatal("a new session went to the agent's older primary stream, not the one its reconnect registered")
		}
	}

	close(second.End) // the newest ends: the older takes the sessions
	<-secondDone
	waitStreams(1)
	if as := d.agentStream("a"); as != older {
		t.Fatal("agent lost although its older stream is still registered")
	}
	close(first.End)
	<-firstDone
	if d.agentStream("a") != nil || len(d.ConnectedAgentIDs()) != 0 {
		t.Fatal("agent still registered after its last stream ended")
	}
}

// TestAuxStreamsBindToPrimary guards auxiliary connections: a stream bound
// with its primary's token joins the agent's rotation without becoming an
// agent stream of its own; a wrong token, another agent's token, or more
// than tcpAgentStreams-1 of them are refused; and they all end when the
// primary does, so a reconnecting agent leaves none behind.
func TestAuxStreamsBindToPrimary(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	primary := &fake.EndableStream{End: make(chan struct{})}
	primaryDone := make(chan struct{})
	go func() { _ = d.RegisterStream("a", primary, directory.Session{}, nil); close(primaryDone) }()
	waitStreamsOf(t, d, "a", 1)
	token := primary.SentHeader().Get(TCPBindHeader)
	if len(token) != 1 || len(token[0]) != 32 {
		t.Fatalf("primary header %v, want a %s token", primary.SentHeader(), TCPBindHeader)
	}

	for _, tc := range []struct{ agent, token string }{{"a", "not-a-token"}, {"b", token[0]}} {
		if err := d.RegisterAuxStream(tc.agent, tc.token, &fake.EndableStream{End: make(chan struct{})}); status.Code(err) != codes.NotFound {
			t.Fatalf("agent %s binding with %q: %v, want NotFound", tc.agent, tc.token, err)
		}
	}

	auxDone := make(chan error, tcpAgentStreams-1)
	auxes := make([]*fake.EndableStream, tcpAgentStreams-1)
	for i := range auxes {
		auxes[i] = &fake.EndableStream{End: make(chan struct{})}
		go func() { auxDone <- d.RegisterAuxStream("a", token[0], auxes[i]) }()
	}
	waitStreamsOf(t, d, "a", tcpAgentStreams)
	for _, s := range auxes {
		for deadline := time.Now().Add(2 * time.Second); s.SentHeader() == nil && time.Now().Before(deadline); {
			time.Sleep(time.Millisecond) // sent just after the stream joined the rotation
		}
		if got := s.SentHeader().Get(TCPBindHeader); len(got) != 1 || got[0] != token[0] {
			t.Fatalf("aux header %v, want the token echoed", s.SentHeader())
		}
	}
	if err := d.RegisterAuxStream("a", token[0], &fake.EndableStream{End: make(chan struct{})}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("binding past %d auxiliary streams: %v, want ResourceExhausted", tcpAgentStreams-1, err)
	}
	if ids := d.ConnectedAgentIDs(); len(ids) != 1 {
		t.Fatalf("connected agents %v, want one", ids)
	}

	close(primary.End) // the primary ends: its auxiliary streams end with it
	<-primaryDone
	for range auxes {
		select {
		case err := <-auxDone:
			if err != nil {
				t.Fatalf("aux stream ended with %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("an auxiliary stream outlived its primary")
		}
	}
	if d.agentStream("a") != nil || len(d.ConnectedAgentIDs()) != 0 || len(d.bound) != 0 {
		t.Fatal("agent still registered after its primary ended")
	}
}

// TestRetiredAuxStreamHandsSessionsOn guards retirement: a stream retired
// after a session picked it but before the session was added hands the
// session to another stream, and the retired stream ends only once its
// sessions have.
func TestRetiredAuxStreamHandsSessionsOn(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	primary := &fake.EndableStream{End: make(chan struct{})}
	go func() { _ = d.RegisterStream("a", primary, directory.Session{}, nil) }()
	defer close(primary.End)
	waitStreamsOf(t, d, "a", 1)
	auxDone := make(chan struct{})
	go func() {
		_ = d.RegisterAuxStream("a", primary.SentHeader().Get(TCPBindHeader)[0], &fake.EndableStream{End: make(chan struct{})})
		close(auxDone)
	}()
	waitStreamsOf(t, d, "a", 2)
	d.Mu.RLock()
	aux := d.All["a"][1]
	d.Mu.RUnlock()

	busy := newTCPSession()
	if !aux.addSession("busy", busy) {
		t.Fatal("a live aux stream refused a session")
	}
	d.retire("a", aux) // the idle check raced with a pick of this stream
	if got := d.attach("a", aux, "late", newTCPSession()); got == aux || got == nil {
		t.Fatalf("a session for a retired stream went to %p, want another stream", got)
	}
	select {
	case <-auxDone:
		t.Fatal("a retired stream ended while it still had a session")
	case <-time.After(20 * time.Millisecond):
	}
	aux.removeSession("busy")
	select {
	case <-auxDone:
	case <-time.After(2 * time.Second):
		t.Fatal("a retired stream did not end after its last session")
	}
}

func waitStreamsOf(t *testing.T, d *TCPDispatcher, agentID string, want int) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
		d.Mu.RLock()
		n := len(d.All[agentID])
		d.Mu.RUnlock()
		if n == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent %s has %d streams, want %d", agentID, n, want)
		}
	}
}
