// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package tcprig is the TCP-tunnel test rig shared by the tests of
// core/session and core/replicas: a relay (a TCPDispatcher, as type
// parameter D), a gRPC server with the agent link, and an agent. It does not
// import core/session, so session's in-package tests can use it; it links the
// agent client, which the core must not, so it lives outside core/.
package tcprig

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
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

// Upload sends n bytes through a fresh tunnel session and waits for the
// target's count.
func Upload[D Relay](h *Harness[D], tb testing.TB, n int) error {
	c := h.Dial(tb)
	defer func() { _ = c.Close() }()
	if _, err := c.Write(TargetHeader('u', n)); err != nil {
		return err
	}
	if _, err := io.Copy(c, io.LimitReader(&PatternReader{}, int64(n))); err != nil {
		return err
	}
	var out [8]byte
	if _, err := io.ReadFull(c, out[:]); err != nil {
		return fmt.Errorf("Upload ack: %w", err)
	}
	if got := binary.BigEndian.Uint64(out[:]); got != uint64(n) { // #nosec G115 -- a test payload size
		return fmt.Errorf("target read %d bytes, sent %d", got, n)
	}
	return nil
}

// Download reads n bytes from a fresh session until EOF and verifies them.
func Download[D Relay](h *Harness[D], tb testing.TB, n int) error {
	c := h.Dial(tb)
	defer func() { _ = c.Close() }()
	if _, err := c.Write(TargetHeader('d', n)); err != nil {
		return err
	}
	buf := make([]byte, 64<<10)
	total := 0
	for {
		k, err := c.Read(buf)
		for i := range k {
			if buf[i] != byte((total+i)%251) {
				return fmt.Errorf("Download byte %d corrupt", total+i)
			}
		}
		total += k
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if total != n {
		return fmt.Errorf("downloaded %d bytes, want %d", total, n)
	}
	return nil
}

// Echo round-trips payload through a fresh session and compares.
func Echo[D Relay](h *Harness[D], tb testing.TB, payload []byte) error {
	c := h.Dial(tb)
	defer func() { _ = c.Close() }()
	if _, err := c.Write(TargetHeader('e', 0)); err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() {
		_, err := c.Write(payload)
		errc <- err
	}()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		return fmt.Errorf("Echo read: %w", err)
	}
	if err := <-errc; err != nil {
		return err
	}
	for i := range got {
		if got[i] != payload[i] {
			return fmt.Errorf("Echo byte %d = %d, want %d", i, got[i], payload[i])
		}
	}
	return nil
}

// RunConcurrent runs fn for sessions concurrently and returns the first error.
func RunConcurrent(sessions int, fn func() error) error {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		first error
	)
	for range sessions {
		wg.Go(func() {
			if err := fn(); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return first
}

// HeapSampler records the peak live heap while it runs.
type HeapSampler struct {
	peak atomic.Uint64
	stop chan struct{}
	done chan struct{}
}

func StartHeapSampler() *HeapSampler {
	s := &HeapSampler{stop: make(chan struct{}), done: make(chan struct{})}
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	go func() {
		defer close(s.done)
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for {
			metrics.Read(sample)
			if v := sample[0].Value.Uint64(); v > s.peak.Load() {
				s.peak.Store(v)
			}
			select {
			case <-s.stop:
				return
			case <-t.C:
			}
		}
	}()
	return s
}

func (s *HeapSampler) Stop() uint64 {
	close(s.stop)
	<-s.done
	return s.peak.Load()
}

// WaitGoroutines waits until the goroutine count drops to at most want.
func WaitGoroutines(want int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		n := runtime.NumGoroutine()
		if n <= want {
			return n, nil
		}
		if time.Now().After(deadline) {
			return n, errors.New("goroutines did not drain")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Relay is the dispatcher the rig serves agents with: *session.TCPDispatcher.
type Relay interface {
	RegisterStream(agentID string, stream pb.Dispatcher_ConnectTCPServer, s directory.Session, onControl func(*pb.TCPData)) error
	RegisterAuxStream(agentID, token string, stream pb.Dispatcher_ConnectTCPServer) error
	DataPlaneListener(inner net.Listener) net.Listener
	AgentLinkQUIC(pc net.PacketConn, tc *tls.Config) (serve func(context.Context) error, err error)
}

// Options bind the rig to a dispatcher type.
type Options[D Relay] struct {
	// New makes the relay; auxIdle, when positive, is its auxiliary idle time.
	New func(logger *slog.Logger, auxIdle time.Duration) D
	// BindHeader is the request header that binds an auxiliary stream
	// (session.TCPBindHeader).
	BindHeader string
	// HandleConn routes conn to the agent's target (TCPDispatcher.HandleConn
	// with a TCP endpoint); RouteConnection as an E2EE vhost session.
	HandleConn      func(ctx context.Context, d D, conn net.Conn, agentID, target string)
	RouteConnection func(ctx context.Context, d D, conn net.Conn, agentID string)
	// Streams is how many streams agentID has registered, and how many of
	// them are data connections; AnyStream whether any agent has one.
	Streams   func(d D, agentID string) (total, data int)
	AnyStream func(d D) bool
}

const AgentID = "datapath-agent"

// AgentHeader in an agent's request metadata names it, for a
// harness relay serving several agents (AgentID without it).
const AgentHeader = "x-harness-agent"

type connectTCPStub struct {
	pb.UnimplementedDispatcherServer
	d    Relay
	bind string
	old  HarnessRelay
	ag   HarnessAgent
	// endPrimary, once closed, ends the agent's primary streams as a relay
	// that drops them would (their registration ends after).
	endPrimary chan struct{}
}

// HarnessRelay selects an older relay's ConnectTCP response header.
type HarnessRelay int

const (
	RelayCurrent     HarnessRelay = iota
	RelayOneStream                // no header, as relays before multi-stream (round 2)
	RelayStreamsOnly              // streams but no bind token, as round 2-3 relays
	RelayNoDataPlane              // bind token, no data connections, as round 4 relays
	RelayLinkV1                   // data connections of link version 1 only, no credit on gRPC streams, as relays before link phase 1b
)

// HarnessAgent selects what further connections the agent opens.
type HarnessAgent int

const (
	AgentCurrent   HarnessAgent = iota // the agent link where offered (agent_link.go), else as AgentDataConns
	AgentDataConns                     // data connections where offered, else auxiliary gRPC ones, as agents before the agent link
	AgentAuxOnly                       // auxiliary gRPC connections only, as round 4 agents
	AgentOneConn                       // every stream on its one connection, as agents before round 4
	AgentDataFails                     // a data connection cannot be opened (an HTTPS proxy)
	AgentQUIC                          // the agent link over QUIC (the relay listens on UDP too)
)

// older reports whether ag stands for an agent from before per-session
// credit on gRPC streams (link phase 1b): it never asks for it.
func (ag HarnessAgent) older() bool { return ag == AgentAuxOnly || ag == AgentOneConn }

// ConnectTCP serves the agent's stream as the production handler does
// (Dispatcher.ConnectTCP), minus authentication.
// allow:wrapper (implements pb.DispatcherServer)
func (s connectTCPStub) ConnectTCP(st pb.Dispatcher_ConnectTCPServer) error {
	if s.old != RelayCurrent || s.ag.older() {
		// Credit on gRPC streams is newer than both: the agent's ask never
		// reaches the relay, so neither side keeps it.
		md, _ := metadata.FromIncomingContext(st.Context())
		md = md.Copy()
		delete(md, dataplane.CreditHeader)
		st = ctxStream{st, metadata.NewIncomingContext(st.Context(), md)}
	}
	if s.old == RelayLinkV1 {
		st = linkV1Stream{st}
	}
	if s.ag != AgentCurrent && s.ag != AgentDataFails && s.ag != AgentQUIC {
		// Older agents never ask for the agent link.
		md, _ := metadata.FromIncomingContext(st.Context())
		md = md.Copy()
		delete(md, dataplane.AgentLinkHeader)
		st = ctxStream{st, metadata.NewIncomingContext(st.Context(), md)}
	}
	id := AgentID
	if md, _ := metadata.FromIncomingContext(st.Context()); len(md.Get(AgentHeader)) > 0 {
		id = md.Get(AgentHeader)[0] // one harness relay, several agents
	}
	if md, _ := metadata.FromIncomingContext(st.Context()); len(md.Get(s.bind)) > 0 {
		return s.d.RegisterAuxStream(id, md.Get(s.bind)[0], st)
	}
	switch s.old {
	case RelayOneStream:
		st = noHeaderStream{st}
	case RelayStreamsOnly:
		st = noBindStream{st, s.bind}
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
type noBindStream struct {
	pb.Dispatcher_ConnectTCPServer
	bind string
}

func (s noBindStream) SendHeader(md metadata.MD) error {
	md = md.Copy()
	delete(md, s.bind)
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

// StartTunnelHarness starts relay + agent. target is the address the relay
// asks the agent to dial (TCP endpoints); with e2ee set every client conn is
// routed as an E2EE vhost session to backend instead.
func StartTunnelHarness[D Relay](tb testing.TB, o Options[D], target string, e2ee *agent.E2EESetup, backend string) *Harness[D] {
	tb.Helper()
	return StartTunnelHarnessRTT(tb, o, target, e2ee, backend, 0)
}

// StartTunnelHarnessRTT is StartTunnelHarness with rtt of added latency on
// the agent's gRPC connection (half each way), to measure flow control on
// a long path.
func StartTunnelHarnessRTT[D Relay](tb testing.TB, o Options[D], target string, e2ee *agent.E2EESetup, backend string, rtt time.Duration) *Harness[D] {
	tb.Helper()
	return StartTunnelHarnessWith(tb, o, target, e2ee, backend, rtt, RelayCurrent, AgentCurrent, 0)
}

// StartTunnelHarnessWith is StartTunnelHarnessRTT against an older relay
// (old) or with an older agent (ag); auxIdle, when positive, is the agent
// link's idle time.
func StartTunnelHarnessWith[D Relay](tb testing.TB, o Options[D], target string, e2ee *agent.E2EESetup, backend string, rtt time.Duration, old HarnessRelay, ag HarnessAgent, auxIdle time.Duration) *Harness[D] {
	tb.Helper()
	run := func(ctx context.Context, client pb.DispatcherClient, dial agent.RelayDialer, logger *slog.Logger) {
		if e2ee != nil {
			_ = e2ee.RunTCPTunnel(ctx, client, dial, ctx, "", backend, logger)
			return
		}
		_ = agent.RunTCPTunnel(ctx, client, dial, ctx, "", logger)
	}
	return StartHarness(tb, o, rtt, old, ag, auxIdle, run, func(ctx context.Context, d D, c net.Conn) {
		if e2ee != nil {
			o.RouteConnection(ctx, d, c, AgentID)
			return
		}
		o.HandleConn(ctx, d, c, AgentID, target)
	})
}

// StartHarness starts the relay and the agent (run), and routes each
// client connection to the harness listener with route.
func StartHarness[D Relay](tb testing.TB, o Options[D], rtt time.Duration, old HarnessRelay, ag HarnessAgent, auxIdle time.Duration,
	run func(context.Context, pb.DispatcherClient, agent.RelayDialer, *slog.Logger),
	route func(context.Context, D, net.Conn)) *Harness[D] {
	tb.Helper()
	logger := slog.New(slog.DiscardHandler)
	d := o.New(logger, auxIdle)
	h := &Harness[D]{D: d, opts: o, EndPrimary: make(chan struct{})}

	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	srv := grpc.NewServer(append(grpcfactory.ProtocolOptions(0), grpcfactory.TransportOptions()...)...)
	pb.RegisterDispatcherServer(srv, connectTCPStub{d: d, bind: o.BindHeader, old: old, ag: ag, endPrimary: h.EndPrimary})
	cert := testcert.Generate(tb)
	lis := tlsconfig.WrapGRPCListener(countingListener{grpcLn, &h.Conns, &h.Writes}, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2"}})
	if old == RelayCurrent || old == RelayLinkV1 {
		lis = d.DataPlaneListener(lis) // as server_run.go
	}
	var serveQUIC func(context.Context) error
	if ag == AgentQUIC {
		pc, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			tb.Fatal(err)
		}
		if serveQUIC, err = d.AgentLinkQUIC(pc, &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}}); err != nil {
			tb.Fatal(err)
		}
	}
	h.Srv = srv
	go func() { _ = srv.Serve(lis) }()

	grpcAddr := grpcLn.Addr().(*net.TCPAddr)
	if rtt > 0 {
		grpcAddr = StartDelayProxy(tb, grpcAddr.String(), rtt/2)
	}
	dialRelay := func() (*grpc.ClientConn, error) {
		return grpcclient.NewMTLSClientConn("127.0.0.1", grpcAddr.Port, &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, NextProtos: []string{"h2"}}) // #nosec G402 -- test-only self-signed relay cert //nolint:gosec
	}
	cc, err := dialRelay()
	if err != nil {
		tb.Fatal(err)
	}
	var dial agent.RelayDialer
	switch ag {
	case AgentDataFails:
		dial.Data = func(context.Context) (net.Conn, error) { return nil, errors.New("proxied") }
		dial.GRPC = dialRelay
	case AgentQUIC:
		dial.QUIC = &agent.QUICDialer{Host: "127.0.0.1", TLS: &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true}} // #nosec G402 -- test-only self-signed relay cert //nolint:gosec
		dial.Transport = agent.TransportQUIC
		fallthrough
	case AgentCurrent, AgentDataConns:
		dial.Data = func(ctx context.Context) (net.Conn, error) {
			return grpcclient.DialConn(ctx, false, &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true}, "127.0.0.1", grpcAddr.Port) // #nosec G402 -- test-only self-signed relay cert //nolint:gosec
		}
		fallthrough
	case AgentAuxOnly:
		dial.GRPC = dialRelay
	}
	ctx, cancel := context.WithCancel(context.Background())
	if serveQUIC != nil {
		qctx, stopQUIC := context.WithCancel(ctx) // #nosec G118 -- cancelled by h.StopQUIC
		h.StopQUIC = stopQUIC
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

	h.RelayLn = relayLn
	h.GRPCAddr = grpcLn.Addr().String()
	// The rest open once the agent is busy (an HTTP agent dials its first
	// data connection at once).
	WaitUntil(tb, func() bool { // AgentID's, or another name's (AgentHeader)
		return o.AnyStream(h.D)
	}, func() string { return "the agent registered no stream" })
	tb.Cleanup(func() { fake.GoroutineProfiles(tb) }) // first: the rig still runs
	return h
}

func WaitUntil(tb testing.TB, ok func() bool, msg func() string) {
	tb.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			tb.Fatal(msg())
		}
	}
}

func StartTarget(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	return ServeTargets(tb, ln)
}

func ServeTargets(tb testing.TB, ln net.Listener) string {
	stop := make(chan struct{})
	tb.Cleanup(func() {
		_ = ln.Close()
		close(stop)
	})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go ServeTarget(c, stop)
		}
	}()
	return ln.Addr().String()
}

func ServeTarget(c net.Conn, stop <-chan struct{}) {
	defer func() { _ = c.Close() }()
	var hdr [9]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		return
	}
	n := int64(binary.BigEndian.Uint64(hdr[1:])) // #nosec G115 -- a test payload size
	switch hdr[0] {
	case 'u':
		got, _ := io.Copy(io.Discard, io.LimitReader(c, n))
		var out [8]byte
		binary.BigEndian.PutUint64(out[:], uint64(got)) // #nosec G115 -- a test payload size
		_, _ = c.Write(out[:])
	case 'd':
		_, _ = io.Copy(c, io.LimitReader(&PatternReader{}, n))
	case 'e':
		_, _ = io.Copy(c, c)
	case 'h':
		<-stop
	}
}

// PatternReader yields byte(off % 251) at stream offset off so receivers
// can verify order and completeness.
type PatternReader struct{ off int }

func (p *PatternReader) Read(b []byte) (int, error) {
	for i := range b {
		b[i] = byte((p.off + i) % 251) // #nosec G115 -- a modulus of 251
	}
	p.off += len(b)
	return len(b), nil
}

func TargetHeader(mode byte, n int) []byte {
	h := make([]byte, 9)
	h[0] = mode
	binary.BigEndian.PutUint64(h[1:], uint64(n)) // #nosec G115 -- a test payload size
	return h
}

// StartDelayProxy forwards TCP to upstream and delivers every chunk delay
// after it was read, in each direction: a delay line without a bandwidth
// limit, so a single transfer is bounded by flow-control windows / RTT.
func StartDelayProxy(tb testing.TB, upstream string, delay time.Duration) *net.TCPAddr {
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
			go DelayCopy(up, down, delay)
			go DelayCopy(down, up, delay)
		}
	}()
	return ln.Addr().(*net.TCPAddr)
}

func DelayCopy(dst, src net.Conn, delay time.Duration) {
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

// Harness is a running rig: the relay, the agent and the client listener.
type Harness[D Relay] struct {
	D        D
	RelayLn  net.Listener
	GRPCAddr string       // the relay's gRPC address (a peer pod dials it for cross-pod sessions)
	Conns    atomic.Int32 // the agent's open gRPC connections
	Writes   atomic.Int64 // the relay's socket writes to the agent's connections
	opts     Options[D]
	// EndPrimary ends the agent's primary stream (connectTCPStub).
	EndPrimary chan struct{}
	Srv        *grpc.Server // the relay's gRPC server: its GracefulStop closes the port, as a drain does
	StopQUIC   func()       // ends the QUIC agent link's serve ctx (Run's ctx: the drain's start)
}

// Dial opens one client connection through the relay to the agent.
func (h *Harness[D]) Dial(tb testing.TB) net.Conn {
	tb.Helper()
	c, err := net.Dial("tcp", h.RelayLn.Addr().String())
	if err != nil {
		tb.Fatalf("dial relay: %v", err)
	}
	return c
}

// DataStreams is how many of the agent's streams are data connections.
func (h *Harness[D]) DataStreams() int {
	_, n := h.opts.Streams(h.D, AgentID)
	return n
}

// Streams is how many streams the agent has registered.
func (h *Harness[D]) Streams() int {
	n, _ := h.opts.Streams(h.D, AgentID)
	return n
}

func (h *Harness[D]) WaitStreams(tb testing.TB, want int) {
	tb.Helper()
	WaitUntil(tb, func() bool { return h.Streams() == want }, func() string { return fmt.Sprintf("agent registered %d streams, want %d", h.Streams(), want) })
}

func (h *Harness[D]) WaitConns(tb testing.TB, want int) {
	tb.Helper()
	WaitUntil(tb, func() bool { return int(h.Conns.Load()) == want }, func() string { return fmt.Sprintf("agent has %d gRPC connections, want %d", h.Conns.Load(), want) })
}

// ConnectTCPStub serves ConnectTCP over d as the production handler does,
// minus authentication; bind is the auxiliary-stream bind header.
func ConnectTCPStub(d Relay, bind string) pb.DispatcherServer {
	return connectTCPStub{d: d, bind: bind}
}
