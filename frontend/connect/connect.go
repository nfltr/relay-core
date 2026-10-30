// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package connect is the client-sessions frontend: the owner's sessions to
// its agents' ports and services that clients open over HTTP on the relay's
// proxy port (`nfltr tcp-connect`, `nfltr dial`, the connect sidecar,
// `nfltr ssh-proxy`): a data connection (data.go), an HTTP/2 stream
// (stream.go), a WebSocket or a hijacked HTTP/1.1 connection, all at
// /connect/. Every session goes through the core's Reach (Core.Reach): the
// composition decides who a request's caller is (Frontend.Caller), the
// core who it may reach.
package connect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/internal/platform/wsconn"
	"github.com/nfltr/relay-core/platform/clientgen"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// Path is the route's prefix: /connect/{agentID}/{port or service}, and
// the data connection's upgrade (dataplane.UpgradePath).
const Path = "/connect/"

// Config is the frontend's part of the relay's settings.
type Config struct {
	Enabled             bool // ssh.proxy.enabled or tcp.enabled
	MinClientGeneration int  // clientgen.Min
	E2EEPolicy          string
}

// Frontend serves /connect/ (ServeHTTP) on the composition's HTTP server.
type Frontend struct {
	Core   *session.Core
	Logger *slog.Logger
	Config func() Config
	// Caller is the principal r's sessions open as (Core.Open's caller):
	// zero when r carries no credential the composition accepts.
	Caller func(r *http.Request) identity.Principal
	// Hold holds a session under caller's credential: a revocation or the
	// credential's expiry ends the returned context; nil, or ok false: the
	// session is not held.
	Hold func(ctx context.Context, caller identity.Principal) (held context.Context, release func(), ok bool)
	// Audit records a session's open (result "allowed" or "denied"); nil:
	// none.
	Audit func(r *http.Request, agentID string, open *pb.TCPData, result, why string)
	// Clients are the data connections served here, drained when the relay
	// leaves (drain.go).
	Clients Clients
}

// ServeHTTP serves a /connect/ request: a data connection, or one
// tunnel to an agent's port as an HTTP/2 stream, a WebSocket or a hijacked
// HTTP/1.1 connection (`ssh -o ProxyCommand="nfltr ssh-proxy ..."`).
//
// Security: only the agent's owner reaches it (Core.Reach; anyone else
// gets 404, as for an agent that does not exist); the agent ID is
// path-validated and a port is 1-65535, dialed on the operator's target
// host (ssh.proxy.target_host, default localhost) or a managed capability's
// address.
func (f *Frontend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// POST over HTTP/2 carries the tunnel as one stream (stream.go).
	stream := IsStream(r)
	if r.Method != http.MethodGet && r.Method != http.MethodConnect && !stream {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cfg := f.Config()
	if !cfg.Enabled {
		http.Error(w, "TCP proxy is not enabled on this server (set tcp.enabled=true)", http.StatusNotFound)
		return
	}
	// The client generation (clientgen), before anything is looked up:
	// every nfltr client of v2.0.0 or later sends it here. The data
	// connection of clients before it is gone; they fall back to HTTP/2
	// tunnels, which the generation setting takes or refuses.
	if dataplane.IsOldUpgrade(r) {
		metrics.RecordDataConnection(1, "connect")
		http.Error(w, agentwire.ClientGenerationTooOld, http.StatusUpgradeRequired)
		return
	}
	if !clientgen.Admit(cfg.MinClientGeneration, agentwire.ParseGeneration(r.Header.Values(agentwire.GenerationKey)), "http", r.UserAgent()) {
		http.Error(w, agentwire.ClientGenerationTooOld, http.StatusUpgradeRequired)
		return
	}
	caller := f.Caller(r)
	if caller.Provider == "" {
		http.Error(w, "authentication required", http.StatusUnauthorized)
		return
	}
	// tcp-connect's tunnels as sessions of one connection (data.go).
	if dataplane.IsUpgrade(r) && r.URL.Path == dataplane.UpgradePath {
		f.serveData(w, r, caller, cfg)
		return
	}

	path := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, Path), "/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 {
		http.Error(w, "usage: /connect/{agentID}/{port}", http.StatusBadRequest)
		return
	}
	if f.Core.Reach == nil {
		http.Error(w, "TCP tunneling is not enabled (set tcp.enabled=true)", http.StatusServiceUnavailable)
		return
	}
	// These tunnels (released clients) are plain: the operator's E2EE
	// policy and the service's mode refuse them as on a data connection,
	// with the reason in the body the client prints.
	open := &pb.TCPData{AgentId: parts[0], TargetAddr: parts[1]}
	agentID, targetAddr, err := f.Core.Reach(r.Context(), caller, session.Target{Agent: parts[0], Service: parts[1]})
	var refusal *session.Refusal
	switch {
	case errors.As(err, &refusal) && e2eeReason(refusal.Reason):
		metrics.RecordConnectSession(false, refusal.Reason.String())
		f.audit(r, refusal.AgentID, open, "denied", refusal.Reason.String()+": "+refusal.Msg)
		http.Error(w, refusal.Msg+"; this tunnel is plain (nfltr before v2.1.6 opens no other): update nfltr (nfltr update)", http.StatusUpgradeRequired)
		return
	case errors.As(err, &refusal):
		metrics.RecordConnectSession(false, refusal.Reason.String())
		http.Error(w, refusal.Msg, Status(refusal.Reason))
		return
	case err == nil && f.Core.TCPDispatcher == nil:
		err = errors.New("TCP tunneling is not enabled (set tcp.enabled=true)")
		fallthrough
	case err != nil:
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	// Per-tunnel lines are Debug: a client opening a connection per request
	// (HTTP over tcp-connect) made them a fifth of the relay's CPU. The
	// transfer metrics (path connect) count and time every tunnel; failures stay Warn.
	logger := logging.With(f.Logger, "agent_id", agentID, "target", targetAddr, "remote", r.RemoteAddr)
	logger.Debug("ssh proxy: connecting to agent")

	// A key's tunnel is held under the key for its whole life: a revocation
	// or the key's expiry ends ctx, which closes the agent's side of the
	// tunnel (newDialedSession), and so the tunnel, on every transport.
	parent := r.Context()
	if f.Hold != nil {
		if held, release, ok := f.Hold(parent, caller); ok {
			defer release()
			parent = held
		}
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	retries, interval := f.Core.DialPolicy()
	agentConn, err := f.Core.TCPDispatcher.DialAgentWithRetry(ctx, agentID, targetAddr, retries, interval)
	if err != nil {
		logger.Warn("ssh proxy: dial agent failed", "error", err)
		metrics.RecordConnectSession(false, relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE.String())
		http.Error(w, "agent temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	defer agentConn.Close() //nolint:errcheck
	metrics.RecordConnectSession(false, "open")
	f.audit(r, agentID, open, "allowed", "")

	began := metrics.TransferBegin(metrics.PathConnect)
	defer metrics.TransferEnd(metrics.PathConnect, began)
	agentConn = metrics.CountConn(agentConn, metrics.PathConnect, began, true, true)
	switch {
	case stream:
		serveStream(w, r, agentConn)
	// WebSocket: connections through CDNs (e.g. Cloudflare) that carry no
	// raw TCP after a hijacked 200.
	case strings.EqualFold(r.Header.Get("Upgrade"), "websocket"):
		ws, err := wsconn.Upgrade(w, r)
		if err != nil {
			logger.Error("ssh proxy: WebSocket upgrade failed", "error", err)
			return
		}
		defer ws.Close() //nolint:errcheck
		bridgeWSToConn(ws, agentConn)
	default:
		serveHijacked(w, agentConn, logger)
	}
	logger.Debug("ssh proxy: tunnel closed")
}

// serveHijacked hijacks the HTTP/1.1 connection, answers 200 and joins it
// to the agent's side as raw bytes.
func serveHijacked(w http.ResponseWriter, agentConn net.Conn, logger *slog.Logger) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "server does not support connection hijacking", http.StatusInternalServerError)
		return
	}
	clientConn, buf, err := hj.Hijack()
	if err != nil {
		logger.Error("ssh proxy: hijack failed", "error", err)
		return
	}
	defer clientConn.Close() //nolint:errcheck
	if _, err := clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n")); err != nil {
		return
	}
	if buf != nil && buf.Reader.Buffered() > 0 { // what the client sent with its request
		if _, err := io.CopyN(agentConn, buf, int64(buf.Reader.Buffered())); err != nil {
			return
		}
	}
	_ = peerlink.Join(clientConn, agentConn)
}

// e2eeReason reports whether reason is an E2EE policy refusal.
func e2eeReason(reason relayv1.ErrorReason) bool {
	return reason == relayv1.ErrorReason_ERROR_REASON_E2EE_REQUIRED || reason == relayv1.ErrorReason_ERROR_REASON_E2EE_UNAVAILABLE
}

// Status is the HTTP status a /connect tunnel answers a refusal with.
func Status(reason relayv1.ErrorReason) int {
	switch reason {
	case relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT:
		return http.StatusBadRequest
	case relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED:
		return http.StatusUnauthorized
	case relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE:
		return http.StatusNotFound
	case relayv1.ErrorReason_ERROR_REASON_E2EE_REQUIRED, relayv1.ErrorReason_ERROR_REASON_E2EE_UNAVAILABLE:
		return http.StatusUpgradeRequired
	}
	return http.StatusServiceUnavailable
}

// audit is Audit when set.
func (f *Frontend) audit(r *http.Request, agentID string, open *pb.TCPData, result, why string) {
	if f.Audit != nil {
		f.Audit(r, agentID, open, result, why)
	}
}

// AuditDetail is the detail of a session's audit record: its mode, e2ee
// (TLS the agent terminates, opaque to the relay) or plain.
func AuditDetail(open *pb.TCPData, why string) string {
	detail := fmt.Sprintf("target=%s e2ee=%t", open.GetTargetAddr(), open.GetE2Ee())
	if why != "" {
		detail += " " + why
	}
	return detail
}

// bridgeWSToConn relays data both ways between a WebSocket connection and
// the agent's side, binary frames unwrapped and wrapped. It returns once
// one side closed.
func bridgeWSToConn(ws *wsconn.Conn, conn net.Conn) {
	done := make(chan struct{}, 2)
	go func() { // WS -> agent
		defer func() { done <- struct{}{} }()
		for {
			opcode, payload, err := ws.ReadMessage()
			if err != nil {
				return
			}
			switch opcode {
			case wsconn.OpClose:
				return
			case wsconn.OpPing:
				_ = ws.WriteMessage(wsconn.OpPong, payload)
				continue
			case wsconn.OpPong:
				continue
			}
			if _, err := conn.Write(payload); err != nil {
				return
			}
		}
	}()
	go func() { // agent -> WS
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 64*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if err := ws.WriteMessage(wsconn.OpBinary, buf[:n]); err != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	<-done
	_ = ws.Close()
	_ = conn.Close()
	<-done
}

// RequestKey is the key r presents: X-API-Key, or Authorization: Bearer or
// ApiKey; header names where it was.
func RequestKey(r *http.Request) (key, header string) {
	if key = r.Header.Get("X-API-Key"); key != "" {
		return key, "X-API-Key"
	}
	auth := r.Header.Get("Authorization")
	if len(auth) > len("Bearer ") && strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		return auth[len("Bearer "):], "Authorization"
	}
	if k, ok := strings.CutPrefix(auth, "ApiKey "); ok && k != "" {
		return k, "Authorization"
	}
	return "", ""
}

// RequestPrincipal authenticates the key r presents (RequestKey), else
// its TLS client certificate, with a; ok is false when it presents none or
// no provider accepts it.
func RequestPrincipal(a *identity.Authenticator, r *http.Request, c identity.Credential) (p identity.Principal, header string, ok bool) {
	c.Token, header = RequestKey(r)
	if c.Token == "" {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			return identity.Principal{}, "", false
		}
		c.PeerCerts = r.TLS.PeerCertificates
	}
	p, err := a.Authenticate(r.Context(), c)
	return p, header, err == nil
}
