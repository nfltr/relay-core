// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

// The share gate of E2EE connections. The relay cannot read an
// E2EE session, so a share's policy is split: the relay checks the IP
// allowlist when it accepts the connection; the agent checks Basic Auth,
// the Bearer token and required headers (the agent SDK's RequireSharePolicy). A
// share with such credentials goes only to an agent stream that names the
// share in its ConnectTCP request metadata (core/session's
// tcpSharePolicyHeader); any other is refused, never passed through
// unchecked.

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/core/sharecode"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

var errShareIPRefused = errors.New("client address is not in the share's IP allowlist")

// admitClient checks the share's IP allowlist for a client at ip.
func (t target) admitClient(ip net.IP) error {
	if t.Share != "" && !t.Policy.Admits(ip) {
		return errShareIPRefused
	}
	return nil
}

// enforcedShare is the share code the agent's stream must name before the
// relay forwards the connection to it; "" when the relay needs no agent
// check (no share, or a share without credentials).
func (t target) enforcedShare() string {
	if t.Share != "" && t.Policy.Gated() {
		return t.Share
	}
	return ""
}

// remoteIP is the IP of a connection's peer.
func remoteIP(a net.Addr) net.IP {
	if ta, ok := a.(*net.TCPAddr); ok {
		return ta.IP
	}
	host, _, _ := net.SplitHostPort(a.String())
	return net.ParseIP(host)
}

// routeE2EE resolves sni and routes conn (peeked replayed first) to its
// agent through d, after the share gate. It closes conn. Both SNI fronts
// (muxListener, Router) route through it.
func routeE2EE(ctx context.Context, resolver *resolver, d *session.TCPDispatcher, logger *slog.Logger, conn net.Conn, sni string, peeked []byte) {
	// An unresolved name is refused, never handed to the relay's own TLS.
	target, err := resolver.resolve(sni)
	if errors.Is(err, errShareStore) {
		logger.Warn("sni: share store unavailable; e2ee connection refused", "sni", sni, "error", err, "remote", conn.RemoteAddr())
		metrics.TransferOpened(metrics.PathSNI, relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE.String())
		_ = conn.Close()
		return
	}
	if err != nil {
		logger.Debug("sni: hostname resolution failed", "sni", sni, "error", err, "remote", conn.RemoteAddr())
		metrics.TransferOpened(metrics.PathSNI, relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND.String())
		_ = conn.Close()
		return
	}
	if err := target.admitClient(remoteIP(conn.RemoteAddr())); err != nil {
		logger.Info("sni: share connection refused", "share", sharecode.ID(target.Share), "agent_id", target.AgentID, "remote", conn.RemoteAddr(), "reason", err)
		metrics.TransferOpened(metrics.PathSNI, relayv1.ErrorReason_ERROR_REASON_SHARE_IP_REFUSED.String())
		_ = conn.Close()
		return
	}
	release, reason := d.AdmitVisitor(target.AgentID, remoteIP(conn.RemoteAddr()))
	if reason != "" {
		logger.Debug("sni: e2ee connection over its account's limits", "agent_id", target.AgentID, "remote", conn.RemoteAddr(), "reason", reason)
		metrics.IncrConnRefusals("sni", reason)
		metrics.TransferOpened(metrics.PathSNI, relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED.String())
		refuseTLS(conn)
		return
	}
	defer release()
	logger.Debug("sni: routing e2ee connection", "sni", sni, "agent_id", target.AgentID, "service", target.Service, "remote", conn.RemoteAddr())

	began := metrics.TransferBegin(metrics.PathSNI)
	defer metrics.TransferEnd(metrics.PathSNI, began)
	// The agent sees the complete ClientHello, then the rest of the handshake.
	// The share's agent's owner pays, whoever the visitor is.
	// Metered under the share's id, never its code (sharecode.ID).
	meter := d.OpenShareMeter(metrics.PathSNI, target.AgentID, sharecode.ID(target.Share))
	defer meter.End()
	visitor := metrics.CountConn(&session.PrefixConn{Conn: conn, Prefix: peeked}, metrics.PathSNI, meter, began, false, true)
	d.RouteConnection(ctx, visitor, target.AgentID, target.Service, target.enforcedShare())
}

// alertAccessDenied is a TLS fatal alert record, access_denied (RFC 8446
// 6.2), in plaintext: the relay holds no key of an E2EE share, so it
// answers a ClientHello over the account's limits with it, and the client
// reports an alert rather than a reset.
var alertAccessDenied = []byte{21, 3, 3, 0, 2, 2, 49}

// refuseTLS writes the alert (bounded: a client that reads nothing does not
// hold the goroutine) and closes conn.
func refuseTLS(conn net.Conn) {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = conn.Write(alertAccessDenied)
	_ = conn.Close()
}
