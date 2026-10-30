// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// Per-agent stream registry and per-session state of the TCP dispatcher
// (dispatcher_tcp.go): an agent's ConnectTCP streams, the auxiliary
// connections a busy agent binds to its primary stream, and the sessions
// spread over them.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/internal/bytequeue"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/metrics"
)

// tcpAgentStreamsHeader is the ConnectTCP response header that asks the
// agent to open that many streams in all (the agent SDK's tcpStreamsHeader).
// New sessions are spread over an agent's streams: each stream has one
// sender and grpc-go's 64 KiB write quota, so with one stream concurrent
// sessions queue behind each other. Plain metadata, no message change:
// older agents ignore it, older relays never send it.
const tcpAgentStreamsHeader = "x-nfltr-tcp-streams"

// tcpAgentStreams is how many streams the relay asks each agent for; 4
// measured +7-8% at 8 sessions over 1 (relay at one CPU).
const tcpAgentStreams = 4

// TCPBindHeader carries the token that binds an auxiliary connection to an
// agent's primary stream (the agent SDK's tcpBindHeader). The relay sends a
// random token in a stream's response header; an agent that knows the
// header opens its further streams each on a connection of its own and
// presents the token in the ConnectTCP request metadata; the relay echoes
// it in the response header once the stream is bound. Every HTTP/2
// connection has one writer and one reader goroutine at each end, so
// streams on separate connections stop sharing them. An older agent
// ignores the header and opens its further streams on its one connection.
const TCPBindHeader = "x-nfltr-tcp-bind"

// tcpAuxIdle is how long an auxiliary stream may carry no session before
// the relay retires it (TCPDispatcher.auxIdle): it takes no new sessions,
// and once its last one ends the stream ends and the agent closes the
// connection. An idle agent is left with its primary connection.
const tcpAuxIdle = 30 * time.Second

// ---------------------------------------------------------------------------
// tcpAgentStream -- per-agent stream state
// ---------------------------------------------------------------------------

type tcpAgentStream struct {
	// SendCh is the agent outbox: frames the server sends to the agent on
	// a gRPC stream (a data connection is written inline: enqueue).
	SendCh chan *pb.TCPData

	// Done is closed when the agent stream ends (RegisterStream returns).
	Done chan struct{}

	// agent is the agent whose stream this is; session names a primary
	// stream in the agent's inventory record (renewals).
	agent   string
	session directory.Session
	// token binds auxiliary connections to this stream (tcpBindHeader);
	// aux counts them (under TCPDispatcher.mu). Empty on an auxiliary stream.
	token string
	aux   int
	// primary is the stream an auxiliary stream is bound to; it ends with
	// it. drained is closed once a retired auxiliary stream has no session.
	primary *tcpAgentStream
	drained chan struct{}
	// link is a primary's agent link (agent_link.go; under agentLink.mu).
	link linkUse
	// silent is closed once a primary's agent link went silent past its
	// liveness timeout (agentLink.silent): the stream ends. nil on an
	// auxiliary stream.
	silent     chan struct{}
	silentOnce sync.Once
	// DC is the data connection an auxiliary stream runs on; nil on a
	// gRPC stream (dispatcher_tcp_dataplane.go).
	DC *dataplane.Conn
	// cr is the stream's per-session credit: dc's, or a gRPC stream's
	// whose agent asked for it (grpcCredit); nil without.
	cr *link.Credit

	Mu       sync.Mutex
	Sessions map[string]*tcpSession
	ending   bool // takes no new session: retired or ended
	opened   int  // sessions added so far, for the idle check
	idleMark int  // opened at the previous idle check

	// shares are the shares whose policy the agent enforces on E2EE
	// sessions (tcpSharePolicyHeader), set at registration of a primary
	// stream. An auxiliary stream carries none of its own: it enforces
	// what its primary declared (enforcesShare).
	shares []string
	// moves: the agent moves off a leaving replica (agent_move.go).
	moves bool
	// movingAway: a rebalancing move of the agent (agent_rebalance.go) is
	// under way or done; once its new session takes the record, this stream
	// stays until the agent closes it with its sessions, as in a drain.
	movingAway atomic.Bool
	// linkSessions counts a primary's sessions open on its agent link
	// (agentLink.open): they are not in Sessions.
	linkSessions atomic.Int32
	// http is what the agent announced of HTTP sessions on a primary
	// stream (dataplane.HTTPSessionsHeader): "" none, "1" beside raw TCP,
	// "only" (agent.HTTPSessionsOnly) without (agent_http_link.go).
	http string
	// answers: the agent answers opens that ask (dataplane.AnswerHeader),
	// set at registration of a primary stream.
	answers bool
}

// NewTCPAgentStream returns an agent's TCP stream with no sessions.
func NewTCPAgentStream() *tcpAgentStream {
	return &tcpAgentStream{
		SendCh:   make(chan *pb.TCPData, 64),
		Done:     make(chan struct{}),
		Sessions: make(map[string]*tcpSession),
	}
}

// send queues a frame into the agent outbox, blocking until space is
// available or the agent stream ends. This provides backpressure to
// callers instead of silently dropping data. sendCh is never closed: a
// session may still be sending when the stream ends.
func (a *tcpAgentStream) send(f *pb.TCPData) {
	if a.cr != nil && isSessionData(f) {
		a.sendData(f)
		return
	}
	a.enqueue(f)
}

func (a *tcpAgentStream) enqueue(f *pb.TCPData) {
	if a.DC != nil {
		// Written inline, or with other sessions' frames when a write is
		// in flight (dataplane.Conn): no hand-off to the sender.
		if a.DC.Send(f) != nil || a.DC.Flush() != nil {
			_ = a.DC.Close()
		}
		return
	}
	select {
	case a.SendCh <- f: // a data connection's bytes are its link connection's (serveDataConn)
		metrics.AddTransferBytes(metrics.PathTCPGRPC, len(f.GetData()), 0)
	case <-a.Done:
	}
}

// addSession adds a session unless the stream takes no more (retired or
// ended); the caller then picks another stream (TCPDispatcher.attach).
func (a *tcpAgentStream) addSession(id string, s *tcpSession) bool {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	if a.ending {
		return false
	}
	a.Sessions[id] = s
	a.opened++
	return true
}

func (a *tcpAgentStream) removeSession(id string) {
	a.Mu.Lock()
	delete(a.Sessions, id)
	a.drainedLocked()
	a.Mu.Unlock()
	if a.cr != nil {
		a.cr.Forget(id)
	}
}

// retire stops the stream taking sessions; drained closes once the last
// one ends.
func (a *tcpAgentStream) retire() {
	a.Mu.Lock()
	a.ending = true
	a.drainedLocked()
	a.Mu.Unlock()
}

func (a *tcpAgentStream) drainedLocked() {
	if a.ending && len(a.Sessions) == 0 && a.drained != nil {
		select {
		case <-a.drained:
		default:
			close(a.drained)
		}
	}
}

// idle reports whether the stream carried no session since the previous
// call.
func (a *tcpAgentStream) idle() bool {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	idle := len(a.Sessions) == 0 && a.opened == a.idleMark
	a.idleMark = a.opened
	return idle
}

// Dispatch routes a frame received from the agent to its session. It
// waits on a session only while that session's client keeps taking bytes,
// and never past stall without (tcpSession.deliver): it resets that session
// instead and reports true.
func (a *tcpAgentStream) Dispatch(frame *pb.TCPData, stall time.Duration) bool {
	id := frame.GetSessionId()
	a.Mu.Lock()
	sess := a.Sessions[id]
	a.Mu.Unlock()
	if sess == nil {
		return false
	}
	if frame.GetClose() {
		if frame.GetHalf() && !a.peerClosedWrite(sess) {
			return false // the client's bytes still go to the agent
		}
		sess.markClosed() // its queued tail is still delivered
		return false
	}
	if data := frame.GetData(); len(data) > 0 && !sess.deliver(data, stall) {
		a.resetSession(id, sess)
		return true
	}
	return false
}

// peerClosedWrite records the agent's half close (TCPData.half): the
// client reads EOF once the queued tail is delivered. It reports whether
// the client's side had ended too: both halves end the session (each side
// ends it on its own; no close follows).
func (a *tcpAgentStream) peerClosedWrite(sess *tcpSession) bool {
	sess.peerHalf.Store(true)
	sess.recvQ.Close()
	return sess.selfHalf.Load()
}

// closeWrite passes the client's half close on (TCPData.half): the
// agent's target reads EOF after the bytes sent before, and the agent's
// bytes keep coming. Once the agent half-closed too, the session ends.
func (a *tcpAgentStream) closeWrite(id string, sess *tcpSession) {
	if sess.selfHalf.Swap(true) {
		return
	}
	a.send(&pb.TCPData{SessionId: id, Close: true, Half: true})
	if sess.peerHalf.Load() {
		sess.markClosed()
	}
}

// resetSession ends a session whose client stopped taking its bytes: the
// agent closes its side, the bytes still queued are discarded, and the
// client's connection is cut (sess.abort), never ended cleanly, so the
// client cannot take a truncated stream for a whole one.
func (a *tcpAgentStream) resetSession(id string, sess *tcpSession) {
	a.Mu.Lock()
	if a.Sessions[id] == sess {
		delete(a.Sessions, id) // its further frames are dropped, not waited on
	}
	a.Mu.Unlock()
	sess.reset.Store(true)
	sess.markClosed()
	for {
		if _, ok := sess.recvQ.Pop(); !ok {
			break
		}
	}
	go a.send(&pb.TCPData{SessionId: id, Close: true}) // the stream's sender may be busy
	if sess.abort != nil {
		sess.abort()
	}
}

// CloseAll tears down all active sessions: the stream ended under them.
// One the agent had not ended (no close, no half close of its own) is
// lost: its client is cut (abort) and its in-process reader gets
// errTCPSessionLost, never the clean end the agent's close gives, since
// the bytes it got may be short of the whole stream
// (TestTCPTunnelStreamLossCutsSessions).
func (a *tcpAgentStream) CloseAll() {
	a.Mu.Lock()
	defer a.Mu.Unlock()
	a.ending = true
	for _, sess := range a.Sessions {
		if sess.lose() && sess.abort != nil {
			sess.abort()
		}
	}
}

// ---------------------------------------------------------------------------
// Registry: an agent's streams
// ---------------------------------------------------------------------------

// RegisterStream is called by the ConnectTCP gRPC handler when an agent opens
// a TCP tunnel stream (its session s, as its record names it).  It blocks
// until the stream ends.
//
// onControl, if non-nil, is called for control frames — TCPData frames whose
// session_id starts with "_".  This allows the Dispatcher to handle protocol
// extensions (e.g. certificate rotation notifications) without coupling the
// TCPDispatcher to specific features.
func (d *TCPDispatcher) RegisterStream(agentID string, stream pb.Dispatcher_ConnectTCPServer, s directory.Session, onControl func(*pb.TCPData)) error {
	as := NewTCPAgentStream()
	as.agent = agentID
	as.session = s
	resume := firstIncoming(stream.Context(), agentLinkResumeHeader)
	as.token = d.resumeToken(agentID, resume)
	as.link.counted = resume != "" && as.token == resume // its connection was counted bound
	as.shares = sharesEnforcedBy(stream.Context())
	as.moves = advertisesMove(stream.Context())
	as.http = httpSessions(stream.Context())
	as.answers = firstIncoming(stream.Context(), dataplane.AnswerHeader) == "1"
	as.cr = grpcCredit(stream.Context(), as.enqueue)
	as.silent = make(chan struct{})
	// Ask the agent for tcpAgentStreams streams in all, on connections
	// bound with the token; one that does not know the headers keeps its
	// one stream, or opens the rest on its one connection. With the data
	// plane on, the further connections may be data connections.
	md := metadata.Pairs(tcpAgentStreamsHeader, strconv.Itoa(tcpAgentStreams), TCPBindHeader, as.token)
	if d.dataPlane.Load() {
		md.Set(dataplane.Header, dataplane.Versions...)
	}
	if as.cr != nil {
		md.Set(dataplane.CreditHeader, strconv.Itoa(tcpGRPCWindow))
	}
	if d.Link != nil {
		md = metadata.Join(md, d.Link.ask(stream.Context(), as))
	}

	d.Mu.Lock()
	d.Streams[agentID] = as
	d.All[agentID] = append(d.All[agentID], as)
	d.bound[as.token] = as
	close(d.registered)
	d.registered = make(chan struct{})
	d.Mu.Unlock()
	// The header goes out once the token binds: the agent dials at once.
	// Sessions placed here meanwhile only queue (the sender starts below).
	_ = stream.SendHeader(md)
	if d.Link != nil {
		d.Link.dialNow(as) // its liveness is the agent's (agent_link.go)
	}

	defer d.unregister(agentID, as)
	// The receiver runs beside; the sender runs here, where it can end the
	// stream (a drain) and renew the agent's record without a goroutine
	// more per agent.
	recvDone := make(chan error, 1)
	go func() { recvDone <- d.recvLoop(as, stream, onControl) }()
	return d.sendLoop(agentID, as, stream, recvDone)
}

// RegisterAuxStream serves a stream an agent opened on an auxiliary
// connection, bound with token to one of its streams (the primary). It
// carries sessions like any other stream of the agent but is not an agent
// session of its own: nothing else about the agent is registered, and it
// ends when the primary ends or after it idled (tcpAuxIdle). An unknown
// token, one of another agent, or one past the primary's tcpAgentStreams-1
// auxiliary streams is refused.
func (d *TCPDispatcher) RegisterAuxStream(agentID, token string, stream pb.Dispatcher_ConnectTCPServer) error {
	var as *tcpAgentStream
	cr := grpcCredit(stream.Context(), func(f *pb.TCPData) { as.enqueue(f) }) // grants follow pops: as is set
	as, err := d.bindAux(agentID, token, nil, cr)
	if err != nil {
		return err
	}
	// The header goes out before the sender starts: a session that picks
	// the stream meanwhile only queues its open frame.
	md := metadata.Pairs(TCPBindHeader, token)
	if cr != nil {
		md.Set(dataplane.CreditHeader, strconv.Itoa(tcpGRPCWindow))
	}
	_ = stream.SendHeader(md)
	return d.serveAux(agentID, as, stream)
}

// bindAux adds an auxiliary stream bound with token to agentID's primary
// stream to the agent's rotation; serveAux serves it.
func (d *TCPDispatcher) bindAux(agentID, token string, dc *dataplane.Conn, cr *link.Credit) (*tcpAgentStream, error) {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	p := d.bound[token]
	if p == nil || !slices.Contains(d.All[agentID], p) {
		return nil, status.Error(codes.NotFound, "no tcp stream of this agent to bind to")
	}
	if p.aux >= tcpAgentStreams-1 {
		return nil, status.Errorf(codes.ResourceExhausted, "an agent binds at most %d auxiliary tcp connections", tcpAgentStreams-1)
	}
	p.aux++
	as := NewTCPAgentStream()
	as.agent, as.primary, as.drained, as.DC, as.cr = agentID, p, make(chan struct{}), dc, cr
	d.All[agentID] = append(d.All[agentID], as)
	return as, nil
}

// serveAux serves a bound auxiliary stream until it ends: its primary
// ended, it retired and its sessions ended, or the stream broke.
func (d *TCPDispatcher) serveAux(agentID string, as *tcpAgentStream, stream tcpFrameStream) error {
	p := as.primary
	defer d.unregister(agentID, as)
	go func() { _ = d.sendLoop(agentID, as, stream, nil) }()
	errc := make(chan error, 1)
	go func() { errc <- d.recvLoop(as, stream, nil) }()
	select {
	case err := <-errc:
		return err
	case <-as.drained: // retired and its sessions ended
		return nil
	case <-p.Done:
		return nil
	}
}

func newBindToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand.Read never fails
	return hex.EncodeToString(b)
}

// unregister removes a stream that ended; an agent stays registered until
// its last stream ends.
func (d *TCPDispatcher) unregister(agentID string, as *tcpAgentStream) {
	close(as.Done)
	d.Mu.Lock()
	d.leaveRotationLocked(agentID, as)
	if as.primary != nil {
		as.primary.aux--
	} else if d.bound[as.token] == as { // not resumed by a newer primary (resumeToken)
		delete(d.bound, as.token)
	}
	d.Mu.Unlock()
	if as.primary == nil && d.Link != nil {
		d.Link.primaryEnded(as)
	}
	as.CloseAll()
	if as.cr != nil {
		as.cr.Close() // senders waiting for credit give up
	}
}

// leaveRotationLocked removes as from agentID's streams; when it was the
// newest primary, the latest primary left, if any, takes its place
// (auxiliary ones end with theirs). Under d.Mu.
func (d *TCPDispatcher) leaveRotationLocked(agentID string, as *tcpAgentStream) {
	rest := slices.DeleteFunc(d.All[agentID], func(s *tcpAgentStream) bool { return s == as })
	if len(rest) == 0 {
		delete(d.All, agentID)
	} else {
		d.All[agentID] = rest
	}
	if d.Streams[agentID] == as {
		delete(d.Streams, agentID)
		for _, s := range slices.Backward(rest) {
			if s.primary == nil {
				d.Streams[agentID] = s
				break
			}
		}
	}
}

// replacedSessionsWait bounds how long a replica keeps a primary stream
// that a newer session of its agent on another replica superseded, for
// the sessions open on it: the replaced client's own bound (relay client
// v0.19.4+ closes the connection once its sessions ended, at most 30 s).
const replacedSessionsWait = 30 * time.Second

// yield takes a primary stream that a newer session of its agent (on
// another replica) superseded out of this replica's routing: new opens go
// where the directory points, the sessions open on it and its auxiliary
// streams run on. It reports whether any is open (the stream then stays
// until they end, replacedSessionsWait at most: sendLoop).
func (d *TCPDispatcher) yield(agentID string, as *tcpAgentStream) bool {
	d.Mu.Lock()
	d.leaveRotationLocked(agentID, as)
	d.Mu.Unlock()
	as.Mu.Lock()
	as.ending = true // a session placed meanwhile picks another stream (attach)
	as.Mu.Unlock()
	return d.carries(agentID, as)
}

// carriesSessions reports whether agentID's TCP stream here carries a
// session.
func (d *TCPDispatcher) carriesSessions(agentID string) bool {
	d.Mu.RLock()
	as := d.Streams[agentID]
	d.Mu.RUnlock()
	return as != nil && d.carries(agentID, primaryOf(as))
}

// carries reports whether the primary stream as, its agent link or an
// auxiliary stream bound to it has a session open.
func (d *TCPDispatcher) carries(agentID string, as *tcpAgentStream) bool {
	if as.linkSessions.Load() > 0 {
		return true
	}
	d.Mu.RLock()
	streams := append([]*tcpAgentStream{as}, d.All[agentID]...)
	d.Mu.RUnlock()
	for _, s := range streams {
		if primaryOf(s) != as {
			continue
		}
		s.Mu.Lock()
		n := len(s.Sessions)
		s.Mu.Unlock()
		if n > 0 {
			return true
		}
	}
	return false
}

// retire takes an idle auxiliary stream out of the agent's rotation; it
// ends once its sessions have (RegisterAuxStream).
func (d *TCPDispatcher) retire(agentID string, as *tcpAgentStream) {
	d.Mu.Lock()
	if rest := slices.DeleteFunc(d.All[agentID], func(s *tcpAgentStream) bool { return s == as }); len(rest) > 0 {
		d.All[agentID] = rest
	} else {
		delete(d.All, agentID)
	}
	d.Mu.Unlock()
	as.retire()
}

// sendLoop drains sendCh into the stream (server -> agent) and sends the
// heartbeats from the same goroutine (one per stream) until the stream ends;
// for an auxiliary stream it also retires it once idle. recvDone is set for
// a primary stream, whose sender runs on the
// handler goroutine: it returns the receiver's end, renews the agent's
// inventory record and lets the agent go during a drain (drain.go).
func (d *TCPDispatcher) sendLoop(agentID string, as *tcpAgentStream, stream tcpFrameStream, recvDone <-chan error) error {
	flush, _ := stream.(interface{ Flush() error })
	// A gRPC stream's context ends with the stream, or earlier when the
	// relay ends it (its credential revoked: revocation.go).
	var ended <-chan struct{}
	var streamCtx context.Context
	if c, ok := stream.(interface{ Context() context.Context }); ok {
		streamCtx = c.Context()
		ended = streamCtx.Done()
	}
	var tick, idle, renew, leave, replaced, replacedBound <-chan time.Time
	var leaving, renewNow <-chan struct{}
	if d.heartbeatInterval > 0 && (as.DC == nil || as.DC.Version() < 2) { // a version-2 link pings itself, as does the agent link (LinkUp)
		ticker := time.NewTicker(d.heartbeatInterval)
		defer ticker.Stop()
		tick = ticker.C
	}
	if as.primary != nil {
		ticker := time.NewTicker(d.AuxIdle)
		defer ticker.Stop()
		idle = ticker.C
	}
	if recvDone != nil {
		if d.renewEvery > 0 && d.renew != nil {
			ticker := time.NewTicker(d.renewEvery)
			defer ticker.Stop()
			renew = ticker.C
			if d.renewNow != nil {
				renewNow = d.renewNow()
			}
		}
		leaving = d.leaving
	}
	for {
		var frame *pb.TCPData
		select {
		case frame = <-as.SendCh:
		case <-tick:
			if d.LinkUp(agentID) {
				continue // the agent link's liveness stands for the agent's
			}
			frame = &pb.TCPData{SessionId: tcpHeartbeatControlSessionID}
		case <-idle:
			if as.idle() {
				d.retire(agentID, as)
				idle = nil
			}
			continue
		case <-renewNow:
			renewNow = d.renewNow()
			if err := d.renew(agentID, as.session); !errors.Is(err, directory.ErrSuperseded) {
				continue
			}
		case <-renew:
			if err := d.renew(agentID, as.session); !errors.Is(err, directory.ErrSuperseded) {
				continue
			}
		case <-replaced:
			if !d.carries(agentID, as) {
				return errReplacedSessionStatus
			}
			continue
		case <-replacedBound:
			return errReplacedSessionStatus
		case <-leaving:
			leaving = nil
			if as.moves && d.podDialer != nil {
				// The agent moves (agent_move.go) and closes this stream with
				// its old connection; its new session takes the record.
				renew, renewNow = nil, nil
				continue
			}
			t := time.NewTimer(drainJitter(d.drainWindow))
			defer t.Stop()
			leave = t.C
			continue
		case <-leave:
			return status.Error(codes.Unavailable, errServerRestarting.Error())
		case err := <-recvDone:
			return err
		case <-ended:
			return context.Cause(streamCtx)
		case <-as.Done:
			return nil
		case <-as.silent:
			return errAgentLinkSilent
		}
		if frame == nil { // superseded: the agent connected to another replica since
			renew, renewNow = nil, nil
			if as.movingAway.Load() {
				continue // moved: the agent closes it
			}
			// A takeover (a rolling update's new pod): the sessions open on
			// this connection finish here, new ones go to the new one
			// (TestTCPTakeoverOnAnotherReplicaLetsOpenSessionsFinish).
			if !d.yield(agentID, as) {
				return errReplacedSessionStatus
			}
			// ponytail: a 500 ms check of a replaced stream's sessions, at
			// most replacedSessionsWait; a signal from removeSession if
			// takeovers become frequent.
			check := time.NewTicker(500 * time.Millisecond)
			defer check.Stop()
			bound := time.NewTimer(replacedSessionsWait)
			defer bound.Stop()
			replaced, replacedBound = check.C, bound.C
			continue
		}
		if err := stream.Send(frame); err != nil {
			return err
		}
		// A data connection buffers frames: one write for all that
		// were queued.
		if flush != nil && len(as.SendCh) == 0 {
			if err := flush.Flush(); err != nil {
				return err
			}
		}
		if frame.GetSessionId() == tcpHeartbeatControlSessionID && !frame.GetOpen() && !frame.GetClose() && len(frame.GetData()) == 0 && d.onHeartbeat != nil {
			d.onHeartbeat(agentID)
		}
	}
}

// recvLoop dispatches frames from the agent (agent -> server) to sessions
// until the stream ends.
func (d *TCPDispatcher) recvLoop(as *tcpAgentStream, stream tcpFrameStream, onControl func(*pb.TCPData)) error {
	for {
		frame, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// Control frames have session IDs starting with "_" and are not
		// dispatched to sessions.  They carry protocol-level signals such
		// as certificate rotation notifications.
		sid := frame.GetSessionId()
		if as.DC == nil {
			metrics.AddTransferBytes(metrics.PathTCPGRPC, 0, len(frame.GetData()))
		}
		if as.DC == nil && as.cr != nil { // a gRPC stream with credit: grants and charges here
			if err := grpcCreditFrame(as.cr, frame); err != nil {
				return err
			}
		}
		if len(sid) > 0 && sid[0] == '_' {
			if onControl != nil && sid != dataplane.WindowSID {
				onControl(frame)
			}
			continue
		}
		if as.Dispatch(frame, d.SessionStall) {
			d.logger.Warn("tcp session reset: its client took none of its bytes",
				"agent_id", as.agent, "session", frame.GetSessionId(), "stall", d.SessionStall)
			metrics.TransferError(streamPath(as), metrics.TransferStalled)
		}
	}
}

// agentStream returns the stream a new session of agentID uses, or nil: one
// of the streams of its newest primary in turn when it opened several, its
// data connections only while it has any. An older primary of the agent
// (the connection a reconnect replaced, which may be dead and not yet
// noticed, or one a move leaves) and its auxiliary streams keep the
// sessions they carry and take no new one.
func (d *TCPDispatcher) agentStream(agentID string) *tcpAgentStream {
	as, _ := d.agentStreamOrSignal(agentID)
	return as
}

// attach adds a session to as or, when as takes no more (an auxiliary
// stream retiring, a stream ending), to another of the agent's streams. It
// returns the stream that took it, or nil when none is left.
func (d *TCPDispatcher) attach(agentID string, as *tcpAgentStream, id string, s *tcpSession) *tcpAgentStream {
	for as != nil && !as.addSession(id, s) {
		as = d.agentStream(agentID) // a stream stops taking sessions only after it left the rotation
	}
	if as != nil && as.cr != nil {
		as.openCredit(id, s) // before the open frame
	}
	return as
}

// agentStreamOrSignal is agentStream plus, when the agent has no stream, the
// channel closed at the next registration.
func (d *TCPDispatcher) agentStreamOrSignal(agentID string) (*tcpAgentStream, <-chan struct{}) {
	d.Mu.RLock()
	defer d.Mu.RUnlock()
	cur := d.Streams[agentID] // the newest primary
	if cur == nil {
		return nil, d.registered
	}
	all := d.All[agentID]
	n, data := 0, 0
	for _, as := range all {
		if primaryOf(as) == cur {
			n++
			if as.DC != nil {
				data++
			}
		}
	}
	if n <= 1 {
		return cur, nil
	}
	// Data connections first: no HTTP/2, and per-session credit.
	pick := d.nextPick.Add(1)
	k := int(pick % uint64(n)) // #nosec G115 -- k < n, a slice length
	if data > 0 {
		k = int(pick % uint64(data)) // #nosec G115 -- k < data, a slice length
	}
	for _, as := range all {
		if primaryOf(as) != cur || data > 0 && as.DC == nil {
			continue
		}
		if k == 0 {
			return as, nil
		}
		k--
	}
	return cur, nil
}

// nextRegistration is closed at the next registration of a primary
// stream here, of any agent.
func (d *TCPDispatcher) nextRegistration() <-chan struct{} {
	d.Mu.RLock()
	defer d.Mu.RUnlock()
	return d.registered
}

// agentStreamWait is how long a connection for an agent that has no stream
// waits for one to register (an agent reconnecting after a relay restart or
// a network blip). It is what the three polls it replaced could wait.
const agentStreamWait = 1500 * time.Millisecond

// streamFor returns agentID's stream on this relay. It waits up to
// agentStreamWait for one to register (an agent reconnecting here) unless the
// inventory places the agent on another replica: a visitor that landed on
// another replica than its agent is forwarded at once, not after the wait.
func (d *TCPDispatcher) streamFor(ctx context.Context, agentID string) *tcpAgentStream {
	if as := d.agentStream(agentID); as != nil {
		return as
	}
	if d.podDialer != nil && d.streamerIPInventory != nil {
		ip, err := d.streamerIPInventory.GetIP(ctx, agentID)
		if err == nil && ip != "" && ip != viper.GetString("loopback") || errors.Is(err, directory.ErrUnavailable) {
			return nil // forwarded; with no directory, refused there at once
		}
	}
	return d.waitAgentStream(ctx, agentID, agentStreamWait)
}

// waitAgentStream returns agentID's stream, waiting up to wait for one to
// register. It wakes on the registration itself, not on a poll interval.
func (d *TCPDispatcher) waitAgentStream(ctx context.Context, agentID string, wait time.Duration) *tcpAgentStream {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		as, registered := d.agentStreamOrSignal(agentID)
		if as != nil {
			return as
		}
		select {
		case <-registered:
		case <-timer.C:
			return d.agentStream(agentID)
		case <-ctx.Done():
			return nil
		}
	}
}

// ---------------------------------------------------------------------------
// tcpSession -- per-connection state
// ---------------------------------------------------------------------------

// tcpSession is one TCP connection within an agent stream. The agent's
// bytes for its client wait in recvQ until the client takes them: a client
// connection's writer (handleLocalConn), an in-process conn's reader
// (DialAgent) or the replica link session to the replica that holds the
// client (serveLinkTCP).
type tcpSession struct {
	recvQ *bytequeue.Queue
	// queued is what recvQ holds by the count of its only pusher, the
	// stream's receive loop (deliver): pushed, not yet popped. limit is
	// recvQ's.
	queued atomic.Int64
	limit  int64
	room   chan struct{} // a pop made room
	// credit returns a data connection's credit for popped bytes.
	credit func(n int)
	// abort cuts the session's client once the session is reset; set
	// before the session is attached.
	abort func()
	reset atomic.Bool // reset (resetSession), not ended
	lost  atomic.Bool // its stream ended under it (CloseAll), not ended
	// selfHalf: the client half-closed (closeWrite); peerHalf: the agent
	// did (peerClosedWrite). Both end the session (markClosed).
	selfHalf, peerHalf atomic.Bool

	closed    chan struct{}
	closeOnce sync.Once
}

func newTCPSession() *tcpSession {
	s := &tcpSession{recvQ: bytequeue.New(tcpSessionQueueBytes), limit: tcpSessionQueueBytes, room: make(chan struct{}, 1), closed: make(chan struct{})}
	s.recvQ.OnPop(s.popped)
	return s
}

func (s *tcpSession) popped(n int) {
	s.queued.Add(-int64(n))
	select {
	case s.room <- struct{}{}:
	default:
	}
	if s.credit != nil {
		s.credit(n)
	}
}

// deliver queues data for the session's client. It returns at once while
// the queue has room, which is always on a stream with credit (every data
// connection, and a gRPC stream whose agent asked for it, link phase 1b):
// its agent sends a session no more than its window (openCredit). An older
// agent's gRPC stream has no per-session credit, so there a full queue
// makes the stream's receive loop wait for the client to take bytes, and
// deliver reports false once the client took none for stall: the caller
// resets the session rather than let one stuck client hold the stream's
// other sessions any longer.
// ponytail: on such a stream a client slower than its agent still paces the
// stream's other sessions; the agent's upgrade removes the wait.
func (s *tcpSession) deliver(data []byte, stall time.Duration) bool {
	if s.queued.Load() >= s.limit {
		t := time.NewTimer(stall)
		defer t.Stop()
		for s.queued.Load() >= s.limit {
			select {
			case <-s.room:
				t.Reset(stall)
			case <-s.closed:
				return true // the session is ending: nothing more is delivered
			case <-t.C:
				return false
			}
		}
	}
	s.queued.Add(int64(len(data)))
	s.recvQ.Push(data) // never waits: below the limit by the count above
	return true
}

// markClosed records that the session is done (the agent's close, or its
// client went away). Its queue stays poppable until drained, so data sent
// before the close is delivered.
func (s *tcpSession) markClosed() {
	s.closeOnce.Do(func() { close(s.closed) })
	s.recvQ.Close()
}

// lose ends a session its stream ended under; it reports whether the
// session was lost: the agent had neither closed nor half-closed it.
func (s *tcpSession) lose() bool {
	select {
	case <-s.closed:
		return false
	default:
	}
	lost := !s.peerHalf.Load()
	if lost {
		s.lost.Store(true) // before the queue closes: its reader sees it
	}
	s.markClosed()
	return lost
}

// endErr is what a reader of the session's drained queue gets: io.EOF,
// errTCPSessionStalled for a reset session, errTCPSessionLost for a lost
// one.
func (s *tcpSession) endErr() error {
	switch {
	case s.reset.Load():
		return errTCPSessionStalled
	case s.lost.Load():
		return errTCPSessionLost
	}
	return io.EOF
}

// errTCPSessionLost ends a session whose agent stream ended under it: a
// join passes it on as link.CodeConnLost.
var errTCPSessionLost = status.Error(codes.Unavailable, "tcp session lost: the agent's stream ended")

// errTCPSessionStalled ends a session reset because its client took none of
// its bytes for the stall bound (tcpSessionStall).
var errTCPSessionStalled = status.Error(codes.ResourceExhausted, "tcp session reset: its client took none of its bytes")

// streamPath is the transfer path of a session on as: its data
// connection's, or the gRPC frame streams'.
func streamPath(as *tcpAgentStream) metrics.Path {
	if as.DC != nil {
		return metrics.PathTCPDataConn
	}
	return metrics.PathTCPGRPC
}
