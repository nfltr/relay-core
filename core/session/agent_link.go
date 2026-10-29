// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// The agent link (relay proto dataplane's agent link): an agent's sessions
// on a github.com/nfltr/link/peerlink Link, which this relay opens on the
// connections the agent dialled in: link over TLS/TCP on the gRPC port
// (route hands over connections that start with dataplane.AgentLinkMagic)
// or QUIC on agent_link.quic.listen (peerlink/quiclink). A connection
// authenticates with its primary ConnectTCP stream's bind token, is named
// by that token on the Link (so a session goes only to the connections of
// the agent's current primary), and is closed when the primary ends. An
// agent without an agent-link connection gets its sessions on its frame
// streams (gRPC, data connections), as before.
//
// The link is up for its primary's whole life: the relay asks the agent to
// dial it (dataplane.AgentLinkDial on the primary) as the primary
// registers, idle or not, so the link's liveness is the agent's (a link
// that goes silent ends its primary: silent) and the relay needs no
// heartbeat of its own on the agent's streams while the link is up
// (LinkUp). A session for an agent whose link has no connection (still
// dialling, or redialling) asks again and waits at most agentLinkDialWait
// for one, else takes the frame streams; concurrent sessions share the one
// request.

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	"github.com/nfltr/quic-go"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nfltr/relay-core/platform/metrics"
)

// agentLinkMaxConns is how many agent-link connections one primary stream
// binds at most.
const agentLinkMaxConns = tcpAgentStreams

// agentLinkDialWait bounds how long a session waits for an agent link the
// relay asked its agent to dial: QUIC binds in about two round trips, TCP
// in about four, after auto's 300 ms QUIC head start where UDP is blocked.
// A session past it, and every later one until the link binds, takes the
// frame streams.
const agentLinkDialWait = time.Second

// linkUse is a primary's agent link (tcpAgentStream.link, under
// agentLink.mu): on the stream itself, so an idle agent's link costs the
// relay no allocation of its own.
type linkUse struct {
	asked   string        // the transport the primary asked for; "": none (no agent link)
	counted bool          // its agent_link_transport_total is recorded
	ended   bool          // the primary ended
	resumed bool          // a newer primary of the agent took its connections over (resumeToken)
	dial    chan struct{} // closed once a connection binds; nil: no request out
	dialBy  time.Time     // when sessions stop waiting for that request
}

// linkConn is a bound agent-link connection.
type linkConn struct {
	Transport string
}

// agentLink is the TCPDispatcher's side of the agent link.
type agentLink struct {
	d    *TCPDispatcher
	tcp  *peerlink.Node
	ln   *agentLinkListener
	quic *quiclink.Node // nil without a UDP listener
	port int            // quic's UDP port

	Mu    sync.Mutex
	Conns map[string]map[io.Closer]linkConn // by token: each connection, its transport and generation
	offer metadata.MD                       // the response header's offer, made at the first ask

	drained sync.Once
}

// drain drains the agent link (TCP and QUIC) as the gRPC port closes:
// GOAWAY to every agent-link connection (no session opens on it any more:
// the frame streams take an agent's new sessions), while the sessions
// open on it run to the relay's drain deadline (TCPDispatcher.drainBy),
// then each still open is aborted link.CodeDrainDeadline. Without a
// deadline (no drain started: a test, or a port that failed) it closes at
// once. A leaving replica closes its port only once its agents moved
// (core_run.go), so their old links are not cut before the deadline.
func (l *agentLink) drain() {
	l.drained.Do(func() {
		deadline := time.Now()
		if l.d.DrainBy != nil {
			deadline = l.d.DrainBy()
		}
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		var wg sync.WaitGroup
		wg.Go(func() { _ = l.tcp.Drain(ctx) })
		l.Mu.Lock()
		q := l.quic
		l.Mu.Unlock()
		if q != nil {
			wg.Go(func() { _ = q.Drain(ctx) })
		}
		go func() { wg.Wait(); cancel() }()
	})
}

func newAgentLink(d *TCPDispatcher, addr net.Addr) *agentLink {
	l := &agentLink{d: d, Conns: map[string]map[io.Closer]linkConn{},
		ln: &agentLinkListener{conns: make(chan net.Conn), addr: addr, done: make(chan struct{})}}
	l.tcp = mustNode(peerlink.New(peerlink.Config{
		Listener: l.ln,
		Link:     link.Config{Window: TCPDataWindow, Auth: agentLinkTokens{l}},
		Observer: func(string) link.Observer { return agentLinkObserver{l: l} },
		ServedPeer: func(nc net.Conn, id link.Identity) string {
			tc, ok := nc.(*agentLinkConn)
			if !ok {
				return id.Subject
			}
			l.Mu.Lock()
			name := tc.name
			tc.token = id.Subject
			l.Mu.Unlock()
			if name != "" {
				return name
			}
			name, err := l.bind(id.Subject, tc, dataplane.AgentLinkTCP)
			if err != nil {
				_ = tc.Close()
				name = id.Subject + "/refused" // no session opens on it
			}
			l.Mu.Lock()
			tc.name = name
			l.Mu.Unlock()
			return name
		},
	}))
	return l
}

// mustNode is a Node whose configuration is fixed here, its tunables
// validated before (link.ParseLiveness): an error is a bug.
func mustNode[N any](n N, err error) N {
	if err != nil {
		panic(err)
	}
	return n
}

// refuseAgentSessions refuses sessions an agent opens: the relay opens
// them (A2A calls arrive by other paths).
func refuseAgentSessions(_ string, _ []byte, s peerlink.Session) { _ = s.Reset() }

// AgentLinkQUIC takes agent-link connections over QUIC on pc with tc (the
// gRPC port's certificate): primaries are offered it from now on, and
// serve runs it until its ctx ends. It needs the agent link on TCP
// (DataPlaneListener) first.
func (d *TCPDispatcher) AgentLinkQUIC(pc net.PacketConn, tc *tls.Config) (serve func(context.Context) error, err error) {
	l := d.Link
	if l == nil {
		return nil, errors.New("agent link: QUIC needs the gRPC port's data plane")
	}
	if err := l.listenQUIC(pc, tc); err != nil {
		return nil, err
	}
	return func(context.Context) error {
		return l.quic.Serve(refuseAgentSessions) // until the gRPC port closes (drain)
	}, nil
}

// listenQUIC makes the QUIC Node on pc (Serve starts it).
func (l *agentLink) listenQUIC(pc net.PacketConn, tc *tls.Config) error {
	tc = tc.Clone()
	tc.NextProtos = dataplane.AgentLinkALPNs() // FeatureChain where the agent offers it
	tc.MinVersion = tls.VersionTLS13
	l.Mu.Lock()
	defer l.Mu.Unlock()
	q, err := quiclink.New(quiclink.Config{PacketConn: pc, TLS: tc, ServedPeer: l.quicPeer, Window: TCPDataWindow})
	if err != nil {
		return err
	}
	l.quic = q
	l.port = pc.LocalAddr().(*net.UDPAddr).Port
	l.offer = nil
	return nil
}

// quicPeer names an accepted QUIC connection by the bind token its first
// stream carries (dataplane.WriteLinkHello).
func (l *agentLink) quicPeer(q *quic.Conn) (string, error) {
	ctx, cancel := context.WithTimeout(q.Context(), DataPlaneSniffTimeout)
	defer cancel()
	st, err := q.AcceptStream(ctx)
	if err != nil {
		return "", err
	}
	defer st.Close() //nolint:errcheck
	_ = st.SetDeadline(time.Now().Add(DataPlaneSniffTimeout))
	token, err := dataplane.ReadLinkHello(st)
	if err == nil {
		_, err = l.verify(token)
	}
	_ = dataplane.AnswerLinkHello(st, err == nil)
	if err != nil {
		metrics.RecordAgentLinkConnection(dataplane.AgentLinkQUIC, "refused")
		return "", err
	}
	c := quicCloser{q}
	name, err := l.bind(token, c, dataplane.AgentLinkQUIC)
	if err != nil {
		return "", err
	}
	counted := metrics.TrackAgentLinkQUIC(func() (uint64, uint64) {
		st := q.ConnectionStats()
		return st.BytesSent, st.BytesReceived
	})
	go func() {
		<-q.Context().Done()
		l.unbind(token, c)
		counted()
		if linkSilent(context.Cause(q.Context())) {
			l.silent(token, dataplane.AgentLinkQUIC)
		}
	}()
	return name, nil
}

type quicCloser struct{ q *quic.Conn }

func (c quicCloser) Close() error {
	return c.q.CloseWithError(quiclink.CodeDone, "the agent's stream ended")
}

// verify checks a bind token: one of a primary stream on this relay,
// with room for another connection.
func (l *agentLink) verify(token string) (*tcpAgentStream, error) {
	l.d.Mu.RLock()
	p := l.d.bound[token]
	l.d.Mu.RUnlock()
	if p == nil {
		return nil, errors.New("no tcp stream of this agent to bind to")
	}
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if len(l.Conns[token]) >= agentLinkMaxConns {
		return nil, errors.New("the agent binds no more agent-link connections")
	}
	if p.link.ended {
		return nil, errAgentLinkEnded
	}
	return p, nil
}

var errAgentLinkEnded = errors.New("the agent link's primary stream ended")

// bind records an accepted connection of token's primary and returns its
// name on the Link; it wakes the sessions waiting for the link.
func (l *agentLink) bind(token string, c io.Closer, transport string) (string, error) {
	l.d.Mu.RLock()
	p := l.d.bound[token]
	l.d.Mu.RUnlock()
	if p == nil {
		return "", errors.New("no tcp stream of this agent to bind to")
	}
	l.Mu.Lock()
	u := &p.link
	if u.ended {
		l.Mu.Unlock()
		return "", errAgentLinkEnded
	}
	if u.dial != nil {
		close(u.dial)
		u.dial = nil
	}
	m := l.Conns[token]
	if m == nil {
		m = map[io.Closer]linkConn{}
		l.Conns[token] = m
	}
	m[c] = linkConn{Transport: transport}
	asked, count := u.asked, u.asked != "" && !u.counted
	u.counted = true
	l.Mu.Unlock()
	metrics.RecordAgentLinkConnection(transport, "bound")
	if count {
		metrics.RecordAgentLinkTransport(asked, transport)
	}
	return token, nil
}

// upLocked reports whether p's link has a connection.
func (l *agentLink) upLocked(p *tcpAgentStream) bool { return len(l.Conns[p.token]) > 0 }

// LinkUp reports whether agentID's newest primary stream here has its
// agent link up: the link's liveness then stands for the agent's (a link
// that goes silent ends the primary), and the relay neither sends nor
// waits for heartbeats of its own on the agent's streams.
func (d *TCPDispatcher) LinkUp(agentID string) bool {
	if d == nil || d.Link == nil {
		return false
	}
	d.Mu.RLock()
	p := d.Streams[agentID]
	d.Mu.RUnlock()
	if p == nil {
		return false
	}
	d.Link.Mu.Lock()
	defer d.Link.Mu.Unlock()
	return !p.link.ended && d.Link.upLocked(p)
}

// dialNow asks p's agent to dial its agent link as p registers, when p
// asked for one: sessions opening meanwhile wait for it as for any
// request (open).
func (l *agentLink) dialNow(p *tcpAgentStream) {
	l.Mu.Lock()
	u := &p.link
	ask := u.asked != "" && !u.ended && u.dial == nil && !l.upLocked(p)
	if ask {
		u.dial, u.dialBy = make(chan struct{}), time.Now().Add(agentLinkDialWait)
	}
	l.Mu.Unlock()
	if ask {
		p.enqueue(&pb.TCPData{SessionId: dataplane.AgentLinkSID, Data: []byte(dataplane.AgentLinkDial)})
	}
}

func (l *agentLink) unbind(token string, c io.Closer) {
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if m := l.Conns[token]; m != nil {
		delete(m, c)
		if len(m) == 0 {
			delete(l.Conns, token)
		}
	}
}

// agentLinkResumeHeader is relay client's linkResumeHeader: a new primary
// stream asks for the agent link of the primary it replaces, by that
// primary's bind token.
const agentLinkResumeHeader = "x-nfltr-agent-link-resume"

// resumeToken is a new primary stream's bind token: the token of the
// agent's primary it names (agentLinkResumeHeader) when that primary is
// agentID's, here, and its agent link has a connection, which the new
// primary then takes over (its sessions go on, new ones ride it at once:
// an agent reconnecting after its TCP path broke while its QUIC link
// migrated); a new token otherwise. The token is a secret only that
// primary's agent holds.
func (d *TCPDispatcher) resumeToken(agentID, token string) string {
	if token == "" || d.Link == nil {
		return newBindToken()
	}
	d.Mu.RLock()
	p := d.bound[token]
	d.Mu.RUnlock()
	if p == nil || p.agent != agentID {
		return newBindToken()
	}
	l := d.Link
	l.Mu.Lock()
	defer l.Mu.Unlock()
	if p.link.ended || p.link.resumed || !l.upLocked(p) {
		return newBindToken()
	}
	p.link.resumed = true
	return token
}

// silent ends the primary stream of token, whose agent-link connection
// went silent past its liveness timeout: the relay's path to that agent
// connection is dead, so its frame streams, on the same path, are too
// (no session opens on them any more; the agent reconnects). Until the
// agent's new primary registers, an open gets the typed offline refusal,
// not a session on a dead leg.
func (l *agentLink) silent(token, transport string) {
	l.d.Mu.RLock()
	p := l.d.bound[token]
	l.d.Mu.RUnlock()
	if p == nil || p.silent == nil {
		return
	}
	p.silentOnce.Do(func() {
		metrics.RecordAgentLinkConnection(transport, "silent")
		l.d.logger.Info("agent link silent past its liveness timeout: the agent's stream ends", "agent_id", p.agent, "transport", transport)
		close(p.silent)
	})
}

// errAgentLinkSilent ends a primary stream whose agent link went silent.
var errAgentLinkSilent = status.Error(codes.Unavailable, "the agent link went silent past its liveness timeout")

// agentLinkObserver sees the TCP agent link's connections end: one that
// ended silent (link.ErrDeadPeer) ends its primary (silent).
type agentLinkObserver struct {
	link.NopObserver
	l *agentLink
}

func (o agentLinkObserver) ConnDown(id link.Identity, err error) {
	if linkSilent(err) {
		o.l.silent(id.Subject, dataplane.AgentLinkTCP)
	}
}

// linkSilent reports whether an agent-link connection ended because its
// peer went silent past the liveness timeout: link's ErrDeadPeer on TCP,
// QUIC's idle timeout.
func linkSilent(err error) bool {
	return errors.Is(err, link.ErrDeadPeer) || errors.As(err, new(*quic.IdleTimeoutError))
}

// ask notes the transport a primary asked for, for the offer in its
// response header; nil when it asked for none.
func (l *agentLink) ask(ctx context.Context, p *tcpAgentStream) metadata.MD {
	v := metadata.ValueFromIncomingContext(ctx, dataplane.AgentLinkHeader)
	if len(v) == 0 || v[0] == "" {
		return nil
	}
	l.Mu.Lock()
	defer l.Mu.Unlock()
	p.link.asked = v[0]
	if l.offer == nil { // the same for every primary: callers join, never change it
		offer := []string{dataplane.AgentLinkTCP}
		l.offer = metadata.MD{}
		if l.quic != nil && l.port > 0 {
			offer = append(offer, dataplane.AgentLinkQUIC)
			l.offer.Set(dataplane.AgentLinkQUICHeader, strconv.Itoa(l.port))
		}
		l.offer.Set(dataplane.AgentLinkHeader, strings.Join(offer, ","))
	}
	return l.offer
}

// primaryEnded closes the connections primary p bound, and counts a
// primary that asked for the agent link and bound none.
func (l *agentLink) primaryEnded(p *tcpAgentStream) {
	l.Mu.Lock()
	u := &p.link
	var conns map[io.Closer]linkConn
	if !u.resumed {
		conns = l.Conns[p.token]
		delete(l.Conns, p.token)
	}
	u.ended = true
	asked, unbound := u.asked, u.asked != "" && !u.counted
	if u.dial != nil {
		close(u.dial) // its waiters take the frame streams
		u.dial = nil
	}
	l.Mu.Unlock()
	if unbound {
		metrics.RecordAgentLinkTransport(asked, "grpc")
	}
	for c := range conns {
		_ = c.Close()
	}
}

// open opens a session for open to the agent of primary p on its agent
// link: QUIC when it has a connection there, else TCP. Without a
// connection it asks the agent to dial (once for concurrent sessions) and
// waits for one until agentLinkDialWait after the request. ok is false
// when the session is not on the agent link (the frame streams take it).
// release, once the session ended, counts its end (the
// open-session gauge): call it exactly once. ctx carries the session's
// link.Chain, if any (a Forward hop's).
func (l *agentLink) open(ctx context.Context, p *tcpAgentStream, open *pb.TCPData) (s peerlink.Session, release func(), ok bool, err error) {
	if l == nil || p == nil {
		return nil, nil, false, nil
	}
	l.Mu.Lock()
	u := &p.link
	if u.asked != "" && !u.ended && !l.upLocked(p) {
		ask := u.dial == nil
		if ask {
			u.dial, u.dialBy = make(chan struct{}), time.Now().Add(agentLinkDialWait)
		}
		wait, by := u.dial, u.dialBy
		l.Mu.Unlock()
		if ask {
			p.enqueue(&pb.TCPData{SessionId: dataplane.AgentLinkSID, Data: []byte(dataplane.AgentLinkDial)})
		}
		t := time.NewTimer(time.Until(by))
		select {
		case <-wait:
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
		l.Mu.Lock()
	}
	var hasTCP, hasQUIC bool
	for _, c := range l.Conns[p.token] {
		hasTCP = hasTCP || c.Transport == dataplane.AgentLinkTCP
		hasQUIC = hasQUIC || c.Transport == dataplane.AgentLinkQUIC
	}
	var node peerlink.Link
	transport := dataplane.AgentLinkTCP
	switch {
	case hasQUIC && l.quic != nil:
		node, transport = l.quic, dataplane.AgentLinkQUIC
	case hasTCP:
		node = l.tcp
	default:
		l.Mu.Unlock()
		return nil, nil, false, nil
	}
	l.Mu.Unlock()
	header, err := dataplane.OpenHeader(open)
	if err != nil {
		return nil, nil, true, err
	}
	octx, cancel := context.WithTimeout(ctx, agentLinkDialWait)
	defer cancel()
	s, err = node.Open(octx, p.token, header)
	if err != nil {
		l.d.logger.Debug("agent link: open failed; the frame streams take the session", "agent_id", p.agent, "error", err)
		return nil, nil, false, nil
	}
	metrics.RecordAgentLinkSession(transport, sessionKind(open))
	metrics.AddAgentLinkOpenSessions(transport, 1)
	p.linkSessions.Add(1)
	return s, sync.OnceFunc(func() { metrics.AddAgentLinkOpenSessions(transport, -1); p.linkSessions.Add(-1) }), true, nil
}

func sessionKind(open *pb.TCPData) string {
	switch {
	case open.GetTargetAddr() == dataplane.HTTPSessionTarget && !open.GetE2Ee():
		return "http"
	case open.GetE2Ee():
		return "e2ee"
	}
	return "tcp"
}

// agentLinkTokens verifies a TCP agent-link connection's bind token; the
// identity's Subject is the token (the connection's name on the Link).
type agentLinkTokens struct{ l *agentLink }

func (agentLinkTokens) Hello(context.Context, tls.ConnectionState) (byte, []byte, error) {
	return 0, nil, errors.New("the relay does not dial agent-link connections")
}

func (a agentLinkTokens) Verify(_ context.Context, _ tls.ConnectionState, method byte, payload []byte) (link.Identity, error) {
	if method != link.MethodToken {
		metrics.RecordAgentLinkConnection(dataplane.AgentLinkTCP, "refused")
		return link.Identity{}, errors.New("an agent-link connection binds with a token")
	}
	if _, err := a.l.verify(string(payload)); err != nil {
		metrics.RecordAgentLinkConnection(dataplane.AgentLinkTCP, "refused")
		return link.Identity{}, err
	}
	return link.Identity{Method: "token", Subject: string(payload)}, nil
}

// agentLinkListener hands the TCP Node the connections route recognised.
type agentLinkListener struct {
	conns chan net.Conn
	addr  net.Addr
	once  sync.Once
	done  chan struct{}
}

func (a *agentLinkListener) Accept() (net.Conn, error) {
	select {
	case c := <-a.conns:
		return c, nil
	case <-a.done:
		return nil, net.ErrClosed
	}
}

func (a *agentLinkListener) Close() error {
	a.once.Do(func() { close(a.done) })
	return nil
}

func (a *agentLinkListener) Addr() net.Addr { return a.addr }

// agentLinkConn is an accepted TCP agent-link connection: its Close
// unbinds it.
type agentLinkConn struct {
	net.Conn
	l     *agentLink
	once  sync.Once
	token string // its primary's, once bound (under l.mu)
	name  string // its name on the Link, once bound (under l.mu)
}

func (c *agentLinkConn) CloseWrite() error { return PassCloseWrite(c.Conn) }

func (c *agentLinkConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	metrics.AddAgentLinkTCPBytes(0, n)
	return n, err
}

func (c *agentLinkConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	metrics.AddAgentLinkTCPBytes(n, 0)
	return n, err
}

func (c *agentLinkConn) Close() error {
	c.once.Do(func() {
		c.l.Mu.Lock()
		token := c.token
		c.l.Mu.Unlock()
		if token != "" {
			c.l.unbind(token, c)
		}
	})
	return c.Conn.Close()
}

// serveConn hands a connection whose magic route read to the TCP
// Node.
func (l *agentLink) serveConn(c net.Conn) {
	ac := &agentLinkConn{Conn: c, l: l}
	select {
	case l.ln.conns <- ac:
	case <-l.ln.done:
		_ = c.Close()
	}
}

// CloseWrite ends c's writes: its peer reads io.EOF.
func CloseWrite(c net.Conn) {
	if PassCloseWrite(c) != nil {
		_ = c.Close()
	}
}

// primaryOf is the primary stream as belongs to (itself for a primary).
func primaryOf(as *tcpAgentStream) *tcpAgentStream {
	if as != nil && as.primary != nil {
		return as.primary
	}
	return as
}
