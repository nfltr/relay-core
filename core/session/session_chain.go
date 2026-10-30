// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// Sessions the relay forwards are links chained end to end
// (github.com/nfltr/link docs/design/chaining.md): a client's /connect
// session on its data connection, or one another replica forwarded on the
// replica link, goes on through a peerlink.Forward hop whose Route is the
// relay's (its checks, then the agent's link, its frame streams, or the
// replica link toward the agent's replica). Forward answers the incoming
// session with the rest of the path's answer (accepted once the agent
// reached its target, or a refusal with its reason and how many hops
// beyond this relay it came from), then joins the two: bytes, half closes
// and typed resets both ways, no goroutine while idle between two link
// sessions.
//
// Answers travel link's own way (ACCEPT, ABORT) on every leg whose two
// sides negotiated link.FeatureChain. A leg that did not answers in band
// (dataplane.ReadAnswer) when its open asks for one: the agent's gRPC and
// data connection frame streams (not link sessions), and peers released
// before FeatureChain (older clients, agents and replicas). inbandIn and inbandOut adapt those legs to Forward, so
// the answer crosses every mix of legs unchanged. Removal: once
// security.min_client_generation is 3 (every client and agent speaks
// FeatureChain) and no replica without it runs, only the frame streams
// answer in band.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// Paths of forwarded sessions, in metrics and logs.
const (
	ChainConnect = "connect" // a client's data connection session
	chainReplica = "replica" // a session another replica forwarded here
)

// chainRoute opens a forwarded session's next leg, or refuses it (a
// *link.Abort from dataplane.Refusal; link.ErrGoAway: draining; any other
// error: no route). release, when not nil, runs once the joined session
// ended.
type chainRoute func(ctx context.Context) (out peerlink.Session, release func(), err error)

// Forward forwards in along route (peerlink.Forward) and returns once in
// is answered; the join runs on. inband: in's opener asked for an answer
// in band (it negotiated no FeatureChain), which in then gets.
func Forward(logger *slog.Logger, path string, in peerlink.Session, inband bool, route chainRoute) {
	if inband && !AnswersNatively(in) {
		in = &inbandIn{Session: in}
	}
	o := &chainObserver{path: path, logger: logger}
	peerlink.Forward(func(ctx context.Context, _ string, _ []byte, _ peerlink.Session) (peerlink.Session, error) {
		out, release, err := route(ctx)
		o.release = release
		return out, err
	}, link.ForwardConfig{Observer: o})("", nil, in)
}

// AnswersNatively reports whether s answers opens link's own way.
func AnswersNatively(s peerlink.Session) bool {
	cs, ok := s.(peerlink.ChainSession)
	return ok && cs.Answers()
}

// chainObserver records one forwarded session's answer and end: metrics,
// a log line with its trace id for a refusal or an abort, and its release.
type chainObserver struct {
	link.NopObserver
	path    string
	logger  *slog.Logger
	release func()
}

func (o *chainObserver) Hop(e link.HopEvent) {
	switch e.Kind {
	case link.HopAnswered:
		var a *link.Abort
		refused := errors.As(e.Err, &a)
		code, hops := "", uint8(0)
		if refused {
			code, hops = abortCodeName(a), a.Hops
			o.logger.Debug("chain: session refused", "path", o.path, "trace", traceID(e.Chain), "code", code, "hops", hops, "reason", a.Msg, "latency", e.Latency)
		}
		metrics.RecordChainAnswer(o.path, refused, code, hops, e.Latency)
	case link.HopEnded:
		if o.release != nil {
			o.release()
		}
		var a *link.Abort
		if errors.As(e.Err, &a) {
			metrics.RecordChainAbort(o.path, abortCodeName(a), a.Hops)
			o.logger.Debug("chain: session aborted", "path", o.path, "trace", traceID(e.Chain), "code", abortCodeName(a), "hops", a.Hops, "reason", a.Msg, "in", e.In, "out", e.Out)
		}
	}
}

// traceID is a chain's trace id as logged: the same on every hop of one
// session (the origin's), for joining its lines across relays and agents.
func traceID(c link.Chain) string { return hex.EncodeToString(c.Trace[:]) }

var linkCodeNames = map[uint32]string{
	link.CodeUnspecified: "unspecified", link.CodeRefused: "refused", link.CodeDraining: "draining",
	link.CodeHopLimit: "hop_limit", link.CodeNoRoute: "no_route", link.CodeConnLost: "conn_lost",
	link.CodeTimeout: "timeout", link.CodeDrainDeadline: "drain_deadline",
}

// abortCodeName names an abort's code: its ErrorReason, or link's own.
func abortCodeName(a *link.Abort) string {
	if r, ok := dataplane.AbortReason(a); ok {
		return r.String()
	}
	if n, ok := linkCodeNames[a.Code]; ok {
		return n
	}
	return "other"
}

// inbandIn is an incoming session whose opener reads its answer in band
// (dataplane.ReadAnswer: a client or replica without FeatureChain) as a ChainSession for Forward: an accept is the OK answer, a
// refusal the answer with its reason followed by a clean end; a reset
// stays a reset.
type inbandIn struct {
	peerlink.Session
	answered atomic.Bool
}

var _ peerlink.ChainSession = (*inbandIn)(nil)

func (s *inbandIn) Answers() bool { return true }

func (s *inbandIn) Accept() error { return s.AcceptPath(0) }

func (s *inbandIn) AcceptPath(uint8) error {
	if !s.answered.CompareAndSwap(false, true) {
		return nil
	}
	return dataplane.WriteAnswer(s.Session, 0, "")
}

func (s *inbandIn) Refuse(code uint32, msg string) error {
	return s.AbortWith(&link.Abort{Refused: true, Code: code, Msg: msg})
}

func (s *inbandIn) Abort(code uint32, msg string) error {
	return s.AbortWith(&link.Abort{Code: code, Msg: msg})
}

func (s *inbandIn) AbortWith(a *link.Abort) error {
	if !a.Refused || !s.answered.CompareAndSwap(false, true) {
		return s.Session.Reset()
	}
	_ = dataplane.WriteAnswer(s.Session, inbandReason(a), a.Msg)
	return s.Session.Close()
}

func (s *inbandIn) Answered(context.Context) error { return link.ErrNoAnswers }
func (s *inbandIn) Chain() link.Chain              { return link.Chain{} }
func (s *inbandIn) PathLen() uint8                 { return 0 }

// Done is the session's when it has one (Forward stops routing once the
// opener went away).
func (s *inbandIn) Done() <-chan struct{} {
	if d, ok := s.Session.(interface{ Done() <-chan struct{} }); ok {
		return d.Done()
	}
	return nil
}

// inbandReason is a refusal's reason as an in-band answer has it: its
// own, or for link's codes the nearest (a draining hop, else a path that
// did not reach the agent).
func inbandReason(a *link.Abort) relayv1.ErrorReason {
	if r, ok := dataplane.AbortReason(a); ok {
		return r
	}
	if a.Code == link.CodeDraining {
		return relayv1.ErrorReason_ERROR_REASON_RELAY_DRAINING
	}
	return relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE
}

// inbandOut is an outgoing session whose far end answers in band (the
// agent's frame streams; an agent or a replica without FeatureChain) as a
// ChainSession for Forward: Answered reads the answer.
type inbandOut struct {
	peerlink.Session
}

var _ peerlink.ChainSession = (*inbandOut)(nil)

func (s *inbandOut) Answers() bool { return true }

// Answered reads the far end's answer, or gives up when ctx ends (the
// session then closes).
func (s *inbandOut) Answered(ctx context.Context) error {
	type result struct {
		a   *relayv1.SessionAnswer
		err error
	}
	ch := make(chan result, 1)
	go func() { a, err := dataplane.ReadAnswer(s.Session); ch <- result{a, err} }()
	var r result
	select {
	case r = <-ch:
	case <-ctx.Done():
		_ = s.Session.Close()
		return ctx.Err()
	}
	switch {
	case errors.Is(r.err, dataplane.ErrNoAnswer):
		return &link.Abort{Code: link.CodeConnLost, Msg: r.err.Error()}
	case r.err != nil:
		return r.err
	}
	if a := dataplane.AnswerAbort(r.a); a != nil {
		return a
	}
	return nil
}

func (s *inbandOut) Accept() error               { return nil }
func (s *inbandOut) AcceptPath(uint8) error      { return nil }
func (s *inbandOut) Refuse(uint32, string) error { return s.Session.Reset() }
func (s *inbandOut) Abort(uint32, string) error  { return s.Session.Reset() }
func (s *inbandOut) AbortWith(*link.Abort) error { return s.Session.Reset() }
func (s *inbandOut) Chain() link.Chain           { return link.Chain{} }
func (s *inbandOut) PathLen() uint8              { return 0 }

// SessionConn is a link session as the net.Conn a caller outside the
// relay's Forward hops holds (net/http's transport, the browser terminal):
// a net.Conn may be written by goroutines at once, and net/http does (it
// hands an upgraded connection over while its write loop can still be
// inside the request's Write), but a link session takes one writer at a
// time, so Write and CloseWrite are serialised
// (TestSessionConnSerialisesWrites).
type SessionConn struct {
	peerlink.Session
	wmu sync.Mutex
}

// Write writes p on the session, one writer at a time.
func (c *SessionConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.Session.Write(p)
}

// CloseWrite ends the session's writing side, one writer at a time.
func (c *SessionConn) CloseWrite() error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.Session.CloseWrite()
}

// WriteTo is the session's own where it has one (the frame streams join
// their backlog into one write).
func (c *SessionConn) WriteTo(w io.Writer) (int64, error) {
	if wt, ok := c.Session.(io.WriterTo); ok {
		return wt.WriteTo(w)
	}
	return io.Copy(w, struct{ io.Reader }{c.Session})
}

// OnGrant has f called with each window update the agent sends for the
// session, as its reader takes what was written (link's Session.OnGrant):
// a writer whose writes all returned sees them still draining. It reports
// false for a session whose transport reports none (QUIC, the frame
// streams).
func (c *SessionConn) OnGrant(f func(n int)) bool {
	g, ok := c.Session.(interface{ OnGrant(func(int)) })
	if ok {
		g.OnGrant(f)
	}
	return ok
}

// dialedSession is a session DialAgent returned to a caller that is no
// Forward hop (the browser terminal, released clients' tunnels, the agent
// HTTP transport): its close runs release once, and ctx's end closes it.
type dialedSession struct {
	SessionConn
	once    sync.Once
	release func()
	stop    func() bool
}

func newDialedSession(ctx context.Context, s peerlink.Session, release func()) *dialedSession {
	d := &dialedSession{SessionConn: SessionConn{Session: s}, release: release}
	d.stop = context.AfterFunc(ctx, func() { _ = d.Close() })
	return d
}

func (d *dialedSession) end() {
	d.once.Do(func() {
		d.stop()
		if d.release != nil {
			d.release()
		}
	})
}

func (d *dialedSession) Close() error {
	err := d.Session.Close()
	d.end()
	return err
}

func (d *dialedSession) Reset() error {
	err := d.Session.Reset()
	d.end()
	return err
}

// ErrFor is a refusal for route errors that carry none: a *link.Abort and
// a draining hop pass as they are (Forward reads them), anything else is
// reason with err's message.
func ErrFor(err error, reason relayv1.ErrorReason, format string, args ...any) error {
	var a *link.Abort
	if errors.As(err, &a) || errors.Is(err, link.ErrGoAway) || errors.Is(err, peerlink.ErrDraining) {
		return err
	}
	return dataplane.Refusal(reason, fmt.Sprintf(format, args...))
}
