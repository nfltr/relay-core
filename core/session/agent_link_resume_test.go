// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay-core/internal/platform/testcert"
	"github.com/nfltr/relay-core/tlsconfig"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestAgentLinkResumedByTheNextPrimary: the real agent's primary TCP
// stream ends on its side while the relay has not noticed yet (its gRPC
// path broke, the agent link did not: spec/Handoff.tla Register with a
// resume, then Adopt); the agent's next primary asks for the link
// (agentLinkResumeHeader) and the relay hands it the old token, so a
// session open on the link goes on, and a new one rides it at once. The
// old primary's end, noticed later, leaves the link alone (resumed).
// When the relay noticed first (refused), the next primary gets a new
// token: the agent drains its parked link (AdoptRefused), the session on
// it ends, and a new session works (spec/README.md finding 5).
func TestAgentLinkResumedByTheNextPrimary(t *testing.T) {
	for _, refused := range []bool{false, true} {
		t.Run(map[bool]string{false: "resumed", true: "refused"}[refused], func(t *testing.T) {
			testAgentLinkResume(t, refused)
		})
	}
}

func testAgentLinkResume(t *testing.T, refused bool) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	const id = "resume-agent"
	logger := slog.New(slog.DiscardHandler)
	td := NewTCPDispatcher(logger, nil, nil)
	d.TCPDispatcher = td
	r := &cutPrimaryRelay{Dispatcher: d, cut: make(chan struct{}), release: make(chan struct{}), oldEnded: make(chan struct{}), waitOld: refused}
	defer r.releaseOld()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cert := testcert.Generate(t)
	lis := td.DataPlaneListener(tlsconfig.WrapGRPCListener(ln, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}}))
	srv := grpc.NewServer(grpcfactory.ProtocolOptions(0)[1], grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		md = md.Copy()
		md.Set("x-apikey-agentID", id)
		return h(srv, identityStream{ss, testPrincipal(metadata.NewIncomingContext(ss.Context(), md))})
	}))
	pb.RegisterDispatcherServer(srv, r)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	target := startTarget(t)
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	a := agent.New(agent.Config{ServerAddr: ln.Addr().String(), AgentID: id, TCPTunnel: true, Retries: 1, Logger: logger,
		TLS: true, TLSConfig: &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h2"}}}) // #nosec G402 -- test-only self-signed relay cert //nolint:gosec
	if err := a.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer a.Close() //nolint:errcheck

	// A session on the agent link, held open across the reconnect.
	held := dialEndpoint(t, td, id, target)
	defer held.Close() //nolint:errcheck
	if _, err := held.Write(targetHeader('e', 0)); err != nil {
		t.Fatal(err)
	}
	echoOn(t, held, "before")
	old := td.agentStream(id)
	waitUntil(t, func() bool { return tokenConns(td, old.token) > 0 }, func() string { return "no agent link bound to the primary" })

	close(r.cut) // the agent's primary ends; the relay's goes on
	if refused {
		r.releaseOld() // the relay notices first: nothing to resume
	}
	waitUntil(t, func() bool { p := td.agentStream(id); return p != nil && p != old }, func() string { return "the agent's next primary did not register" })
	if p := td.agentStream(id); (p.token == old.token) == refused {
		t.Fatalf("the next primary's token %q, old %q: resumed %v, want %v", p.token, old.token, p.token == old.token, !refused)
	}
	if refused {
		_ = held.SetReadDeadline(time.Now().Add(10 * time.Second))
		if _, err := held.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("the session on the parked link did not end: %v", err)
		}
	} else {
		echoOn(t, held, "after the resume") // the link's session went on
		r.releaseOld()                      // the relay notices the old primary's end
		waitUntil(t, func() bool { return !streamListed(td, id, old) }, func() string { return "the old primary is still listed" })
		echoOn(t, held, "after the old primary ended")
		if tokenConns(td, old.token) == 0 {
			t.Fatal("the old primary's end closed the resumed link")
		}
	}
	c := dialEndpoint(t, td, id, target) // a new session
	defer c.Close()                      //nolint:errcheck
	if _, err := c.Write(targetHeader('e', 0)); err != nil {
		t.Fatal(err)
	}
	echoOn(t, c, "a new session")
}

// cutPrimaryRelay serves the agent's first primary ConnectTCP so that
// closing cut ends it for the agent while the relay's stream goes on
// (its sends dropped) until release: a gRPC path that broke towards the
// agent before the relay noticed.
type cutPrimaryRelay struct {
	*Dispatcher
	cut, release chan struct{}
	oldEnded     chan struct{} // the held primary's ConnectTCP returned
	waitOld      bool          // a later primary waits for oldEnded: the relay noticed first
	first        sync.Once
	releaseOnce  sync.Once
}

func (r *cutPrimaryRelay) releaseOld() { r.releaseOnce.Do(func() { close(r.release) }) }

func (r *cutPrimaryRelay) ConnectTCP(st pb.Dispatcher_ConnectTCPServer) error {
	held := false
	if len(metadata.ValueFromIncomingContext(st.Context(), TCPBindHeader)) == 0 {
		r.first.Do(func() { held = true })
	}
	if !held {
		if r.waitOld && len(metadata.ValueFromIncomingContext(st.Context(), TCPBindHeader)) == 0 {
			<-r.oldEnded
		}
		return r.Dispatcher.ConnectTCP(st)
	}
	hs := heldStream{Dispatcher_ConnectTCPServer: st, ctx: context.WithoutCancel(st.Context()), cut: r.cut, release: r.release}
	done := make(chan error, 1)
	go func() {
		defer close(r.oldEnded)
		done <- r.Dispatcher.ConnectTCP(hs)
	}()
	select {
	case err := <-done:
		return err
	case <-r.cut:
		return status.Error(codes.Unavailable, "test: the agent's path to this primary broke")
	}
}

// heldStream is the relay's side of a primary whose path broke: after cut
// its sends go nowhere and its receive waits for release, then reads EOF.
type heldStream struct {
	pb.Dispatcher_ConnectTCPServer
	ctx          context.Context
	cut, release <-chan struct{}
}

func (s heldStream) Context() context.Context { return s.ctx }

func (s heldStream) Send(f *pb.TCPData) error {
	select {
	case <-s.cut:
		return nil
	default:
		return s.Dispatcher_ConnectTCPServer.Send(f)
	}
}

func (s heldStream) Recv() (*pb.TCPData, error) {
	f, err := s.Dispatcher_ConnectTCPServer.Recv()
	if err != nil {
		select {
		case <-s.cut:
			<-s.release
			return nil, io.EOF
		default:
		}
	}
	return f, err
}

// dialEndpoint opens a session to the agent's target through td.
func dialEndpoint(t *testing.T, td *TCPDispatcher, agentID, target string) net.Conn {
	t.Helper()
	c, relaySide := net.Pipe()
	go td.HandleConn(t.Context(), relaySide, TCPEndpoint{AgentID: agentID, TargetAddr: target})
	return c
}

// echoOn round-trips msg on an echo session.
func echoOn(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("%s: write: %v", msg, err)
	}
	got := make([]byte, len(msg))
	if _, err := io.ReadFull(c, got); err != nil || string(got) != msg {
		t.Fatalf("%s: echo %q, %v", msg, got, err)
	}
	_ = c.SetDeadline(time.Time{})
}

func tokenConns(td *TCPDispatcher, token string) int {
	td.Link.Mu.Lock()
	defer td.Link.Mu.Unlock()
	return len(td.Link.Conns[token])
}

func streamListed(td *TCPDispatcher, agentID string, as *tcpAgentStream) bool {
	td.Mu.RLock()
	defer td.Mu.RUnlock()
	for _, s := range td.All[agentID] {
		if s == as {
			return true
		}
	}
	return false
}
