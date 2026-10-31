// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

// Router accepts TLS connections on a port of its own, peeks the TLS
// ClientHello for the Server Name Indication (SNI), maps the host name to
// an agent and forwards the raw encrypted bytes through the core's TCP
// sessions. The relay never terminates TLS for E2EE traffic: it sees only
// the SNI host name and opaque TLS records; the agent terminates TLS.
//
// An SNI host name is a share's E2EE URL: the configured base domain
// stripped leaves a share code, which maps to the share's agent:
//
//	<code>.e2ee.example.com → the share's agent  (base_domain = "e2ee.example.com")
//
// Nothing else resolves: a host name naming an agent directly carries no
// credential the relay can check at the TLS layer, and only a share link
// reaches an agent without a login (agent_reach.go). Unresolved → closed.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/acceptloop"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// target is where an E2EE host name leads: the share's grant and code,
// which the relay applies before forwarding (gate.go).
type target struct {
	frontend.Grant
	Share string
}

// resolver strips a configured base domain suffix and maps the remaining
// share code to the share's agent, so a share works as an E2EE URL like
// <code>.e2ee.nfltr.xyz.
type resolver struct {
	baseDomain string                 // e.g. "e2ee.example.com"
	grants     frontend.GrantVerifier // nil resolves nothing
}

func newResolver(baseDomain string, grants frontend.GrantVerifier) *resolver {
	return &resolver{baseDomain: strings.ToLower(strings.TrimPrefix(baseDomain, ".")), grants: grants}
}

// errNotAShare is the resolution failure of a host name that is no valid
// share code: an agent ID, an expired or unknown code.
var errNotAShare = errors.New("not a share's host name")

// errShareStore is the resolution failure of a share host name the share
// store could not answer for (e.g. Redis unreachable): the connection is
// refused like an unknown code's, but logged and counted apart.
var errShareStore = errors.New("share store unavailable")

// resolve strips the base domain suffix and returns the share and its agent.
func (r *resolver) resolve(sniHostname string) (target, error) {
	host := strings.ToLower(sniHostname)
	suffix := "." + r.baseDomain
	if !strings.HasSuffix(host, suffix) {
		return target{}, fmt.Errorf("SNI %q does not match base domain %q", sniHostname, r.baseDomain)
	}
	prefix := strings.TrimSuffix(host, suffix)
	if prefix == "" || r.grants == nil {
		return target{}, fmt.Errorf("SNI %q: %w", sniHostname, errNotAShare)
	}
	g, err := r.grants.Grant(prefix)
	if errors.Is(err, frontend.ErrNoGrant) {
		return target{}, fmt.Errorf("SNI %q: %w", sniHostname, errNotAShare)
	}
	if err != nil {
		return target{}, fmt.Errorf("SNI %q: %w: %w", sniHostname, errShareStore, err)
	}
	return target{Grant: g, Share: prefix}, nil
}

// Router serves E2EE TLS passthrough on a listener of its own.
type Router struct {
	resolver      *resolver
	tcpDispatcher *session.TCPDispatcher
	logger        *slog.Logger

	mu       sync.Mutex
	listener net.Listener
}

// NewRouter routes the shares under baseDomain (grants: nil resolves
// nothing) to their agents through d.
func NewRouter(baseDomain string, grants frontend.GrantVerifier, d *session.TCPDispatcher, logger *slog.Logger) *Router {
	return &Router{resolver: newResolver(baseDomain, grants), tcpDispatcher: d, logger: logger}
}

// Serve accepts TLS connections on ln, extracts SNI, and routes to agents.
// Blocks until ctx is cancelled or ln is closed.
func (s *Router) Serve(ctx context.Context, ln net.Listener) error {
	s.mu.Lock()
	s.listener = ln
	s.mu.Unlock()

	s.logger.Info("SNI router listening", "addr", ln.Addr())

	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()

	var backoff acceptloop.Backoff
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if backoff.Retry(s.logger, "sni", err) {
				continue
			}
			return fmt.Errorf("sni router: accept: %w", err)
		}
		backoff.Reset()
		go s.handleConn(ctx, conn)
	}
}

// handleConn reads the TLS ClientHello, extracts SNI, resolves the agent ID,
// and routes the connection (including the already-read bytes) to the agent.
func (s *Router) handleConn(ctx context.Context, conn net.Conn) {
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	start := time.Now()

	// Set a deadline for the ClientHello read — don't let slow clients
	// hold connections indefinitely.
	if err := conn.SetReadDeadline(time.Now().Add(session.SNIPeekTimeout)); err != nil {
		s.logger.Debug("sni: failed to set read deadline", "error", err)
		return
	}

	// Peek at the TLS ClientHello without consuming bytes.
	sni, peeked, err := session.ExtractSNI(conn)
	if err != nil {
		s.logger.Debug("sni: failed to extract SNI", "error", err, "remote", conn.RemoteAddr())
		metrics.TransferOpened(metrics.PathSNI, relayv1.ErrorReason_ERROR_REASON_PROTOCOL_UNSUPPORTED.String())
		return
	}

	// Clear the deadline for the proxied connection.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		s.logger.Debug("sni: failed to clear read deadline", "error", err)
		return
	}

	routeE2EE(ctx, s.resolver, s.tcpDispatcher, s.logger, conn, sni, peeked, start)
	conn = nil // routeE2EE closes it
}
