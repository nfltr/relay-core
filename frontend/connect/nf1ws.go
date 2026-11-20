// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/relaystatus"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/wsconn"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"google.golang.org/grpc/codes"
)

// A browser opens a capability address over a WebSocket (nfltr
// docs/decisions/browser-peer-2026-11-16.md section 2, "the relay path"):
// GET NF1WebSocketPath with subprotocol NF1Subprotocol; its first message
// is the address's rid (binary, agentservices.RIDSize bytes), and from
// then on each binary message carries the session's bytes, both ways. The
// session is the anonymous nf1 open of a data connection (the
// same caps, refusals, metering and agent session), so the agent
// cannot tell a browser from the CLI. A refusal is a close code
// (4003 not served, 4004 refused, 4009 balance spent, 1013 try later, 1011
// aborted); the relay sends nothing else of its own. The Origin and
// User-Agent are logged, never trusted: the WebSocket carries no
// credential, and any page holding an address could open it anyway.
const (
	NF1WebSocketPath = "/nf1/v1"
	NF1Subprotocol   = "nf1.v1"
)

// The close codes of a browser's session (RFC 6455 section 7.4; 4000-4999
// private use): the CLI's exits, as nf1.Dial reads the same refusals.
const (
	wsNotServed  uint16 = 4003 // exit 3: unknown, offline, disabled (session.NF1NotServed)
	wsRefused    uint16 = 4004 // exit 4: over a limit, or another refusal with its reason
	wsBalance    uint16 = 4009 // the serving account's balance is spent
	wsTryLater   uint16 = 1013 // no route now (draining, no answer in time): nf1.ErrUnreachable
	wsAborted    uint16 = 1011 // the session failed after it opened (a reset)
	wsIdle       uint16 = 1001 // no byte either way for nf1.websocket_idle
	wsNotBinary  uint16 = 1003 // a text message
	wsNormal     uint16 = 1000
	wsRIDTimeout        = session.DataPlaneSniffTimeout // the rid message's bound, as a data connection's hello
)

// wsCloseCode is the close code and reason of a session ended by a.
func wsCloseCode(a *link.Abort) (uint16, string) {
	reason, app := dataplane.AbortReason(a)
	switch {
	case !a.Refused:
		return wsAborted, a.Msg
	case !app:
		return wsTryLater, a.Msg
	case reason == relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, reason == relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, reason == relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE:
		return wsNotServed, a.Msg
	case reason == relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED:
		return wsBalance, a.Msg
	}
	return wsRefused, reason.String() + ": " + a.Msg
}

// nf1WS are the WebSocket's settings (nf1Limits).
type nf1WS struct {
	enabled bool
	idle    time.Duration
}

// serveNF1WebSocket serves a browser's session at NF1WebSocketPath, with
// no credential (any it carries is ignored) and no client generation
// (a browser sends none). Off (404) unless tcp.enabled, nf1.enabled and
// nf1.websocket.
func (f *Frontend) serveNF1WebSocket(w http.ResponseWriter, r *http.Request, cfg Config) {
	ws := f.nf1Limits().ws
	if !cfg.Enabled || !cfg.NF1 || !ws.enabled || f.Core.TCPDispatcher == nil {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet || !wsconn.OffersProtocol(r, NF1Subprotocol) {
		http.Error(w, "a WebSocket GET with subprotocol "+NF1Subprotocol, http.StatusBadRequest)
		return
	}
	release, ok := f.admitAnonymous(r)
	if !ok {
		relaystatus.WriteHTTP(w, http.StatusTooManyRequests, codes.ResourceExhausted, relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "nf1: too many connections from this address")
		return
	}
	defer release()
	c, err := wsconn.UpgradeProtocol(w, r, NF1Subprotocol)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.Logger.Debug("nf1: websocket", "remote", r.RemoteAddr, "origin", r.Header.Get("Origin"), "user_agent", r.UserAgent())
	s := newWSSession(c, ws.idle)
	defer s.Close() //nolint:errcheck
	_ = c.NetConn().SetReadDeadline(time.Now().Add(wsRIDTimeout))
	rid, err := s.firstMessage()
	_ = c.NetConn().SetReadDeadline(time.Time{})
	if err != nil {
		return
	}
	f.serveNF1Session(r, &pb.TCPData{Nf1Rid: rid, E2Ee: true}, s, true)
	// ponytail: a draining relay does not wait for these sessions (the
	// data connections' Clients); its exit cuts them (1006 at the
	// browser), which then reopens. Add them to Clients if deploys show it.
	<-s.done
}

// wsSession is a browser's WebSocket as the incoming session of an nf1
// open (peerlink.ChainSession): it answers natively, a refusal or an
// abort as its close code (wsCloseCode), an accept as nothing (the
// agent's nf1 answer is the first byte the browser reads). A WebSocket has
// no half close: CloseWrite sends nothing, and the session's own half
// close travels inside its TLS (close_notify); it ends when the browser
// closes, the agent's side ends whole, or no byte moved either way for
// its idle bound.
type wsSession struct {
	c     *wsconn.Conn
	buf   []byte
	idle  time.Duration
	timer *time.Timer // nil: no idle bound
	once  sync.Once
	done  chan struct{}
}

var _ peerlink.ChainSession = (*wsSession)(nil)

func newWSSession(c *wsconn.Conn, idle time.Duration) *wsSession {
	s := &wsSession{c: c, idle: idle, done: make(chan struct{})}
	if idle > 0 {
		s.timer = time.AfterFunc(idle, func() { _ = s.end(wsIdle, "idle") })
	}
	return s
}

// end closes the WebSocket with code once; later calls do nothing.
func (s *wsSession) end(code uint16, reason string) (err error) {
	s.once.Do(func() {
		if s.timer != nil {
			s.timer.Stop()
		}
		err = s.c.CloseWith(code, reason)
		close(s.done)
	})
	return err
}

func (s *wsSession) moved() {
	if s.timer != nil {
		s.timer.Reset(s.idle)
	}
}

// next reads the next data message: io.EOF at the browser's close.
func (s *wsSession) next() ([]byte, error) {
	for {
		op, p, err := s.c.ReadMessage()
		switch {
		case err != nil:
			return nil, err
		case op == wsconn.OpClose:
			return nil, io.EOF
		case op == wsconn.OpPing:
			_ = s.c.WriteMessage(wsconn.OpPong, p)
		case op == wsconn.OpText:
			_ = s.end(wsNotBinary, "binary messages only")
			return nil, errors.New("nf1: a text message")
		case op == wsconn.OpBinary:
			return p, nil
		}
	}
}

// firstMessage is the rid message; its length is serveNF1Session's to
// judge (any but agentservices.RIDSize is "not served").
func (s *wsSession) firstMessage() ([]byte, error) {
	p, err := s.next()
	if err != nil {
		_ = s.end(wsNormal, "")
	}
	return p, err
}

func (s *wsSession) Read(p []byte) (int, error) {
	for len(s.buf) == 0 {
		m, err := s.next()
		if err != nil {
			return 0, err
		}
		s.buf = m
	}
	n := copy(p, s.buf)
	s.buf = s.buf[n:]
	s.moved()
	return n, nil
}

func (s *wsSession) Write(p []byte) (int, error) {
	for n := 0; n < len(p); {
		m := min(len(p)-n, maxWSMessage)
		if err := s.c.WriteMessage(wsconn.OpBinary, p[n:n+m]); err != nil {
			return n, err
		}
		n += m
	}
	s.moved()
	return len(p), nil
}

// maxWSMessage is the data connection's frame bound (link's data frame,
// and wsconn's read bound): a message either way is at most this.
const maxWSMessage = 1 << 20

func (s *wsSession) Close() error                       { return s.end(wsNormal, "") }
func (s *wsSession) CloseWrite() error                  { return nil }
func (s *wsSession) Reset() error                       { return s.end(wsAborted, "reset") }
func (s *wsSession) LocalAddr() net.Addr                { return s.c.NetConn().LocalAddr() }
func (s *wsSession) RemoteAddr() net.Addr               { return s.c.NetConn().RemoteAddr() }
func (s *wsSession) SetDeadline(t time.Time) error      { return s.c.NetConn().SetDeadline(t) }
func (s *wsSession) SetReadDeadline(t time.Time) error  { return s.c.NetConn().SetReadDeadline(t) }
func (s *wsSession) SetWriteDeadline(t time.Time) error { return s.c.NetConn().SetWriteDeadline(t) }

func (s *wsSession) Answers() bool                  { return true }
func (s *wsSession) Accept() error                  { return nil }
func (s *wsSession) AcceptPath(uint8) error         { return nil }
func (s *wsSession) Answered(context.Context) error { return nil }
func (s *wsSession) Chain() link.Chain              { return link.Chain{} }
func (s *wsSession) PathLen() uint8                 { return 0 }
func (s *wsSession) Refuse(code uint32, msg string) error {
	return s.AbortWith(&link.Abort{Refused: true, Code: code, Msg: msg})
}
func (s *wsSession) AbortWith(a *link.Abort) error { return s.end(wsCloseCode(a)) }
