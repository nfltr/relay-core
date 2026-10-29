// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

// In-process harness for the TCP tunnel data path: the relay's TCPDispatcher
// and the agent's runTCPTunnel connected over a real loopback gRPC
// connection built with the production transport options on both ends
// (grpcfactory.TransportOptions and the relay's batching TLS listener,
// grpcclient's TLS client), as in production.
//
// Client conns enter through a relay listener exactly as a public conn does:
// handleConn (TCP endpoints) or RouteConnection (E2EE/SNI vhost).

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay-core/internal/platform/testcert"
	"github.com/nfltr/relay-core/tlsconfig"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/client/grpcclient"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const harnessAgentID = "datapath-agent"

// harnessAgentHeader in an agent's request metadata names it, for a
// harness relay serving several agents (harnessAgentID without it).
const harnessAgentHeader = "x-harness-agent"

type connectTCPStub struct {
	pb.UnimplementedDispatcherServer
	d   *session.TCPDispatcher
	old harnessRelay
	ag  harnessAgent
	// endPrimary, once closed, ends the agent's primary streams as a relay
	// that drops them would (their registration ends after).
	endPrimary chan struct{}
}

// harnessRelay selects an older relay's ConnectTCP response header.
type harnessRelay int

const (
	relayCurrent     harnessRelay = iota
	relayOneStream                // no header, as relays before multi-stream (round 2)
	relayStreamsOnly              // streams but no bind token, as round 2-3 relays
	relayNoDataPlane              // bind token, no data connections, as round 4 relays
	relayLinkV1                   // data connections of link version 1 only, no credit on gRPC streams, as relays before link phase 1b
)

// harnessAgent selects what further connections the agent opens.
type harnessAgent int

const (
	agentCurrent   harnessAgent = iota // the agent link where offered (agent_link.go), else as agentDataConns
	agentDataConns                     // data connections where offered, else auxiliary gRPC ones, as agents before the agent link
	agentAuxOnly                       // auxiliary gRPC connections only, as round 4 agents
	agentOneConn                       // every stream on its one connection, as agents before round 4
	agentDataFails                     // a data connection cannot be opened (an HTTPS proxy)
	agentQUIC                          // the agent link over QUIC (the relay listens on UDP too)
)

// older reports whether ag stands for an agent from before per-session
// credit on gRPC streams (link phase 1b): it never asks for it.
func (ag harnessAgent) older() bool { return ag == agentAuxOnly || ag == agentOneConn }

// ConnectTCP serves the agent's stream as the production handler does
// (Dispatcher.ConnectTCP), minus authentication.
// allow:wrapper (implements pb.DispatcherServer)
func (s connectTCPStub) ConnectTCP(st pb.Dispatcher_ConnectTCPServer) error {
	if s.old != relayCurrent || s.ag.older() {
		// Credit on gRPC streams is newer than both: the agent's ask never
		// reaches the relay, so neither side keeps it.
		md, _ := metadata.FromIncomingContext(st.Context())
		md = md.Copy()
		delete(md, dataplane.CreditHeader)
		st = ctxStream{st, metadata.NewIncomingContext(st.Context(), md)}
	}
	if s.old == relayLinkV1 {
		st = linkV1Stream{st}
	}
	if s.ag != agentCurrent && s.ag != agentDataFails && s.ag != agentQUIC {
		// Older agents never ask for the agent link.
		md, _ := metadata.FromIncomingContext(st.Context())
		md = md.Copy()
		delete(md, dataplane.AgentLinkHeader)
		st = ctxStream{st, metadata.NewIncomingContext(st.Context(), md)}
	}
	id := harnessAgentID
	if md, _ := metadata.FromIncomingContext(st.Context()); len(md.Get(harnessAgentHeader)) > 0 {
		id = md.Get(harnessAgentHeader)[0] // one harness relay, several agents
	}
	if md, _ := metadata.FromIncomingContext(st.Context()); len(md.Get(session.TCPBindHeader)) > 0 {
		return s.d.RegisterAuxStream(id, md.Get(session.TCPBindHeader)[0], st)
	}
	switch s.old {
	case relayOneStream:
		st = noHeaderStream{st}
	case relayStreamsOnly:
		st = noBindStream{st}
	}
	done := make(chan error, 1)
	go func() { done <- s.d.RegisterStream(id, st, directory.Session{}, nil) }()
	select {
	case err := <-done:
		return err
	case <-s.endPrimary:
		return nil
	}
}

// ctxStream replaces a stream's context (its request metadata).
type ctxStream struct {
	pb.Dispatcher_ConnectTCPServer
	ctx context.Context
}

// Context: the replaced one. allow:wrapper
func (s ctxStream) Context() context.Context { return s.ctx }

// linkV1Stream advertises data connections of link version 1 only.
type linkV1Stream struct{ pb.Dispatcher_ConnectTCPServer }

func (s linkV1Stream) SendHeader(md metadata.MD) error {
	if len(md.Get(dataplane.Header)) > 0 {
		md = md.Copy()
		md.Set(dataplane.Header, "1")
	}
	return s.Dispatcher_ConnectTCPServer.SendHeader(md)
}

// noHeaderStream drops SendHeader: headers then go out with the first frame.
type noHeaderStream struct{ pb.Dispatcher_ConnectTCPServer }

func (noHeaderStream) SendHeader(metadata.MD) error { return nil }

// noBindStream drops the bind token from the header.
type noBindStream struct{ pb.Dispatcher_ConnectTCPServer }

func (s noBindStream) SendHeader(md metadata.MD) error {
	md = md.Copy()
	delete(md, session.TCPBindHeader)
	return s.Dispatcher_ConnectTCPServer.SendHeader(md)
}

// countingListener counts the connections it accepted.
type countingListener struct {
	net.Listener
	open   *atomic.Int32
	writes *atomic.Int64 // the relay's writes to the accepted sockets
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.open.Add(1)
	return &countedConn{Conn: c, open: l.open, writes: l.writes}, nil
}

type countedConn struct {
	net.Conn
	open   *atomic.Int32
	writes *atomic.Int64
	once   sync.Once
}

func (c *countedConn) Write(b []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(b)
}

func (c *countedConn) Close() error {
	c.once.Do(func() { c.open.Add(-1) })
	return c.Conn.Close()
}

type tunnelHarness struct {
	d        *session.TCPDispatcher
	relayLn  net.Listener
	grpcAddr string       // the relay's gRPC address (a peer pod dials it for cross-pod sessions)
	conns    atomic.Int32 // the agent's open gRPC connections
	writes   atomic.Int64 // the relay's socket writes to the agent's connections
	// endPrimary ends the agent's primary stream (connectTCPStub).
	endPrimary chan struct{}
	srv        *grpc.Server // the relay's gRPC server: its GracefulStop closes the port, as a drain does
	stopQUIC   func()       // ends the QUIC agent link's serve ctx (Run's ctx: the drain's start)
}

// startHarness starts the relay and the agent (run), and routes each
// client connection to the harness listener with route.
func startHarness(tb testing.TB, rtt time.Duration, old harnessRelay, ag harnessAgent,
	run func(context.Context, pb.DispatcherClient, agent.RelayDialer, *slog.Logger),
	route func(context.Context, *session.TCPDispatcher, net.Conn)) *tunnelHarness {
	tb.Helper()
	logger := slog.New(slog.DiscardHandler)
	d := session.NewTCPDispatcher(logger, nil, nil)
	if harnessAuxIdle > 0 {
		d.AuxIdle = harnessAuxIdle
	}
	h := &tunnelHarness{d: d, endPrimary: make(chan struct{})}

	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	srv := grpc.NewServer(append(grpcfactory.ProtocolOptions(0), grpcfactory.TransportOptions()...)...)
	pb.RegisterDispatcherServer(srv, connectTCPStub{d: d, old: old, ag: ag, endPrimary: h.endPrimary})
	cert := testcert.Generate(tb)
	lis := tlsconfig.WrapGRPCListener(countingListener{grpcLn, &h.conns, &h.writes}, &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}})
	if old == relayCurrent || old == relayLinkV1 {
		lis = d.DataPlaneListener(lis) // as server_run.go
	}
	var serveQUIC func(context.Context) error
	if ag == agentQUIC {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		if serveQUIC, err = d.AgentLinkQUIC(pc, &tls.Config{Certificates: []tls.Certificate{cert}}); err != nil {
			tb.Fatal(err)
		}
	}
	h.srv = srv
	go func() { _ = srv.Serve(lis) }()

	grpcAddr := grpcLn.Addr().(*net.TCPAddr)
	if rtt > 0 {
		grpcAddr = startDelayProxy(tb, grpcAddr.String(), rtt/2)
	}
	dialRelay := func() (*grpc.ClientConn, error) {
		return grpcclient.NewMTLSClientConn("127.0.0.1", grpcAddr.Port, &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}) //nolint:gosec // test-only self-signed relay cert
	}
	cc, err := dialRelay()
	if err != nil {
		tb.Fatal(err)
	}
	var dial agent.RelayDialer
	switch ag {
	case agentDataFails:
		dial.Data = func(context.Context) (net.Conn, error) { return nil, errors.New("proxied") }
		dial.GRPC = dialRelay
	case agentQUIC:
		dial.QUIC = &agent.QUICDialer{Host: "127.0.0.1", TLS: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // test-only self-signed relay cert
		dial.Transport = agent.TransportQUIC
		fallthrough
	case agentCurrent, agentDataConns:
		dial.Data = func(ctx context.Context) (net.Conn, error) {
			return grpcclient.DialConn(ctx, false, &tls.Config{InsecureSkipVerify: true}, "127.0.0.1", grpcAddr.Port) //nolint:gosec // test-only self-signed relay cert
		}
		fallthrough
	case agentAuxOnly:
		dial.GRPC = dialRelay
	}
	ctx, cancel := context.WithCancel(context.Background())
	if serveQUIC != nil {
		qctx, stopQUIC := context.WithCancel(ctx)
		h.stopQUIC = stopQUIC
		go func() { _ = serveQUIC(qctx) }()
	}
	client := pb.NewDispatcherClient(cc)
	agentDone := make(chan struct{})
	go func() {
		defer close(agentDone)
		run(ctx, client, dial, logger)
	}()

	relayLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	go func() {
		for {
			c, err := relayLn.Accept()
			if err != nil {
				return
			}
			go route(ctx, d, c)
		}
	}()

	tb.Cleanup(func() {
		_ = relayLn.Close()
		cancel()
		srv.Stop()
		_ = cc.Close()
		select {
		case <-agentDone:
		case <-time.After(5 * time.Second):
			tb.Error("agent tunnel did not stop after its stream ended")
		}
	})

	h.relayLn = relayLn
	h.grpcAddr = grpcLn.Addr().String()
	// The rest open once the agent is busy (an HTTP agent dials its first
	// data connection at once).
	waitUntil(tb, func() bool { // harnessAgentID's, or another name's (harnessAgentHeader)
		h.d.Mu.RLock()
		defer h.d.Mu.RUnlock()
		return len(h.d.All) > 0
	}, func() string { return "the agent registered no stream" })
	return h
}

// dataStreams is how many of the agent's streams are data connections.
func (h *tunnelHarness) dataStreams() int {
	h.d.Mu.RLock()
	defer h.d.Mu.RUnlock()
	n := 0
	for _, as := range h.d.All[harnessAgentID] {
		if as.DC != nil {
			n++
		}
	}
	return n
}

func waitUntil(tb testing.TB, ok func() bool, msg func() string) {
	tb.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			tb.Fatal(msg())
		}
	}
}

// ---------------------------------------------------------------------------
// Target service: one header byte selects the mode, then an 8-byte length.
//   'u' upload:   read n bytes, reply with the 8-byte count read.
//   'd' download: write n bytes of pattern, then close.
//   'e' echo:     copy everything back until EOF.
//   'h' hold:     never read (a stuck consumer) until the conn closes.
// ---------------------------------------------------------------------------

// patternReader yields byte(off % 251) at stream offset off so receivers
// can verify order and completeness.
type patternReader struct{ off int }

func (p *patternReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte((p.off + i) % 251)
	}
	p.off += len(b)
	return len(b), nil
}

// startDelayProxy forwards TCP to upstream and delivers every chunk delay
// after it was read, in each direction: a delay line without a bandwidth
// limit, so a single transfer is bounded by flow-control windows / RTT.
func startDelayProxy(tb testing.TB, upstream string, delay time.Duration) *net.TCPAddr {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = down.Close()
				continue
			}
			go delayCopy(up, down, delay)
			go delayCopy(down, up, delay)
		}
	}()
	return ln.Addr().(*net.TCPAddr)
}

func delayCopy(dst, src net.Conn, delay time.Duration) {
	type chunk struct {
		due  time.Time
		data []byte
	}
	ch := make(chan chunk, 1<<16)
	go func() {
		defer close(ch)
		for {
			buf := make([]byte, 32<<10)
			n, err := src.Read(buf)
			if n > 0 {
				ch <- chunk{time.Now().Add(delay), buf[:n]}
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { _ = dst.Close() }()
	for c := range ch {
		time.Sleep(time.Until(c.due))
		if _, err := dst.Write(c.data); err != nil {
			return
		}
	}
}
