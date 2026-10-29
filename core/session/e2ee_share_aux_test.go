// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/nfltr/relay-core/core/directory"
)

// TestProtectedShareOnAuxStreams guards the share gate over auxiliary gRPC
// and data connections: an auxiliary stream enforces what its primary
// declared, never what its own request metadata claims. A protected share is refused for an
// agent whose primary did not declare it (on every stream, auxiliary ones
// included) and is routed over the auxiliary streams of one whose primary
// did.
func TestProtectedShareOnAuxStreams(t *testing.T) {
	for name, data := range map[string]bool{"grpc": false, "data connections": true} {
		t.Run(name, func(t *testing.T) { testProtectedShareOnAuxStreams(t, data) })
	}
}

func testProtectedShareOnAuxStreams(t *testing.T, data bool) {
	const share = "swift-bay"
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	dataAddr := ""
	if data {
		dataAddr, _ = startDataPlane(t, d)
	}
	declares := metadata.NewIncomingContext(context.Background(), metadata.Pairs(tcpSharePolicyHeader, share))

	// connect registers agentID's primary (declaring the share or not) and
	// tcpAgentStreams-1 auxiliary streams that all claim the share.
	connect := func(agentID string, primaryCtx context.Context) {
		t.Helper()
		primary := &fake.EndableStream{End: make(chan struct{}), Ctx: primaryCtx}
		t.Cleanup(func() { close(primary.End) })
		go func() { _ = d.RegisterStream(agentID, primary, directory.Session{}, nil) }()
		waitStreamsOf(t, d, agentID, 1)
		token := primary.SentHeader().Get(TCPBindHeader)[0]
		for range tcpAgentStreams - 1 {
			if dataAddr != "" { // a data connection carries no metadata at all
				if _, _, err := dialData(t, dataAddr, token); err != nil {
					t.Fatal(err)
				}
				continue
			}
			aux := &fake.EndableStream{End: make(chan struct{}), Ctx: declares}
			go func() { _ = d.RegisterAuxStream(agentID, token, aux) }()
		}
		waitStreamsOf(t, d, agentID, tcpAgentStreams)
	}
	connect("undeclared", nil)
	connect("declared", declares)

	streamsOf := func(agentID string) []*tcpAgentStream {
		d.Mu.RLock()
		defer d.Mu.RUnlock()
		return append([]*tcpAgentStream(nil), d.All[agentID]...)
	}
	for _, as := range streamsOf("undeclared") {
		if as.enforcesShare(share) {
			t.Fatalf("a stream (aux: %v) of an agent whose primary did not declare the share enforces it", as.primary != nil)
		}
	}

	// One connection per stream: the rotation gives each stream one.
	route := func(agentID string) (ends []net.Conn, done chan struct{}) {
		done = make(chan struct{}, tcpAgentStreams)
		for range tcpAgentStreams {
			client, server := net.Pipe()
			ends = append(ends, client)
			go func() { d.RouteConnection(context.Background(), server, agentID, "", share); done <- struct{}{} }()
		}
		return ends, done
	}

	ends, done := route("undeclared")
	for range tcpAgentStreams {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("a share connection for an agent that does not enforce the share was not refused")
		}
	}
	for _, c := range ends {
		_ = c.Close()
	}
	for _, as := range streamsOf("undeclared") {
		if as.sessionCount() != 0 {
			t.Fatal("a protected share reached a stream of an agent that does not enforce it")
		}
	}

	ends, done = route("declared")
	defer func() {
		for _, c := range ends {
			_ = c.Close() // a half close: the sessions end once the agent's side does
		}
		for _, as := range streamsOf("declared") {
			as.CloseAll()
		}
		for range tcpAgentStreams {
			<-done
		}
	}()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
		aux, onAux, sessions := 0, 0, 0
		for _, as := range streamsOf("declared") {
			sessions += as.sessionCount()
			if as.primary != nil {
				aux++
				if as.sessionCount() > 0 {
					onAux++
				}
			}
		}
		// Every connection attached too: CloseAll below ends the streams
		// with them registered, and a connection still attaching would
		// loop on them (attach), taking no new session as they end.
		if aux == tcpAgentStreams-1 && onAux == aux && sessions == tcpAgentStreams {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d auxiliary streams carry a share session, want all", onAux, aux)
		}
	}
}

// TestShareHandedOnToUnenforcingStreamRefused guards the recheck after
// attach: a session whose picked stream retired before taking it moves to
// another stream of the agent, and is refused when that one does not
// enforce the share.
func TestShareHandedOnToUnenforcingStreamRefused(t *testing.T) {
	const share = "swift-bay"
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	other := &fake.EndableStream{End: make(chan struct{})} // declares nothing
	defer close(other.End)
	go func() { _ = d.RegisterStream("r", other, directory.Session{}, nil) }()
	waitStreamsOf(t, d, "r", 1)

	retired := NewTCPAgentStream()
	retired.primary = &tcpAgentStream{shares: []string{share}}
	retired.retire()

	client, server := net.Pipe()
	defer client.Close() //nolint:errcheck
	done := make(chan struct{})
	go func() {
		d.handleLocalConn(server, TCPEndpoint{AgentID: "r", TargetAddr: "__e2ee__", E2EE: true, Share: share}, retired)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a share session handed on to a stream that does not enforce the share was not refused")
	}
	if as := d.agentStream("r"); as.sessionCount() != 0 {
		t.Fatal("the refused session stayed on the stream")
	}
}

func (a *tcpAgentStream) sessionCount() int {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	return len(a.Sessions)
}
