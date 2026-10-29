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
	"github.com/nfltr/relay-core/platform/metrics"
)

var errShareIPRefused = errors.New("client address is not in the share's IP allowlist")

// Metric error types (dispatcher_sni_router_errors_total) of refused share
// connections.
const (
	sniErrShareIP    = "share_ip_refused"
	sniErrShareStore = "share_store"
)

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
func routeE2EE(ctx context.Context, resolver *resolver, d *session.TCPDispatcher, logger *slog.Logger, conn net.Conn, sni string, peeked []byte, start time.Time) {
	// An unresolved name is refused, never handed to the relay's own TLS.
	target, err := resolver.resolve(sni)
	if errors.Is(err, errShareStore) {
		logger.Warn("sni: share store unavailable; e2ee connection refused", "sni", sni, "error", err, "remote", conn.RemoteAddr())
		metrics.RegisterSNIRouterError(sniErrShareStore)
		_ = conn.Close()
		return
	}
	if err != nil {
		logger.Debug("sni: hostname resolution failed", "sni", sni, "error", err, "remote", conn.RemoteAddr())
		metrics.RegisterSNIRouterError("resolve")
		_ = conn.Close()
		return
	}
	if err := target.admitClient(remoteIP(conn.RemoteAddr())); err != nil {
		logger.Info("sni: share connection refused", "share", target.Share, "agent_id", target.AgentID, "remote", conn.RemoteAddr(), "reason", err)
		metrics.RegisterSNIRouterError(sniErrShareIP)
		_ = conn.Close()
		return
	}
	logger.Debug("sni: routing e2ee connection", "sni", sni, "agent_id", target.AgentID, "service", target.Service, "remote", conn.RemoteAddr())

	metrics.RegisterSNIRouterConnection()
	defer func() {
		metrics.DeregisterSNIRouterConnection()
		metrics.RegisterSNIRouterDuration(time.Since(start).Seconds())
	}()
	// The agent sees the complete ClientHello, then the rest of the handshake.
	d.RouteConnection(ctx, &session.PrefixConn{Conn: conn, Prefix: peeked}, target.AgentID, target.Service, target.enforcedShare())
}
