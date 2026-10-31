// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// SNI Multiplexer (E2EE + gRPC on a single port)
//
// muxListener wraps a raw TCP net.Listener and inspects TLS ClientHello
// SNI to multiplex connections:
//
//   - SNI matching the E2EE base domain suffix → routed to the agent via
//     TCPDispatcher (TLS passthrough, no termination)
//   - All other connections → returned from Accept() with the peeked bytes
//     replayed, so the caller (e.g. tls.NewListener + gRPC) can process them
//     normally
//
// This allows a single external port (443) to serve both gRPC traffic and
// E2EE TLS-passthrough traffic, eliminating the need for a separate E2EE
// listener/load-balancer.
package sni

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/acceptloop"
)

// muxListener implements net.Listener.  It accepts raw TCP connections,
// peeks at the TLS ClientHello to extract the SNI hostname, and routes E2EE
// connections directly to agents while returning all other connections to the
// caller.
//
// Routing decision: with share grants, the mux checks whether the
// subdomain prefix of the SNI hostname is shaped like a share code. This
// allows E2EE URLs like <code>.nfltr.xyz (no .e2ee. subdomain needed).
// Without grants, the mux falls back to static suffix matching (e.g.
// *.e2ee.nfltr.xyz).
type muxListener struct {
	inner         net.Listener
	e2eeSuffix    string // e.g. ".nfltr.xyz"
	resolver      *resolver
	tcpDispatcher *session.TCPDispatcher
	grants        frontend.GrantVerifier // optional; enables share-code-aware routing
	logger        *slog.Logger
	ctx           context.Context
	conns         chan net.Conn // non-E2EE connections, hello replayed
	done          chan struct{} // closed when inner stops accepting
	err           error         // inner's terminal Accept error, set before done
}

// NewMux multiplexes inner: a connection whose SNI is a share's E2EE host
// name under e2eeSuffix goes to the share's agent through d; every other
// one is returned from Accept with its ClientHello replayed.
//
// With grants, the mux checks whether the subdomain prefix is shaped like
// a share code before routing to E2EE — this enables URLs like
// <code>.nfltr.xyz without a .e2ee. subdomain. Without, the mux falls back
// to static suffix matching.
func NewMux(
	ctx context.Context,
	inner net.Listener,
	e2eeSuffix string,
	grants frontend.GrantVerifier,
	d *session.TCPDispatcher,
	logger *slog.Logger,
) net.Listener {
	// Ensure the suffix starts with a dot for clean matching.
	if !strings.HasPrefix(e2eeSuffix, ".") {
		e2eeSuffix = "." + e2eeSuffix
	}
	l := &muxListener{
		inner:         inner,
		e2eeSuffix:    strings.ToLower(e2eeSuffix),
		resolver:      newResolver(e2eeSuffix, grants),
		tcpDispatcher: d,
		grants:        grants,
		logger:        logger,
		ctx:           ctx,
		conns:         make(chan net.Conn),
		done:          make(chan struct{}),
	}
	go l.acceptLoop()
	return l
}

// Accept implements net.Listener.  It blocks until a non-E2EE connection
// arrives or the listener is closed.  E2EE connections are silently routed
// to agents in background goroutines.
func (l *muxListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, l.err
	}
}

// acceptLoop accepts every connection and peeks its ClientHello in a
// goroutine of its own, so a slow or silent client delays only itself, not
// the accepts behind it (the port carries every gRPC reconnect and HTTPS).
func (l *muxListener) acceptLoop() {
	var backoff acceptloop.Backoff
	for {
		conn, err := l.inner.Accept()
		if err != nil {
			if backoff.Retry(l.logger, "sni_mux", err) {
				continue
			}
			l.err = err
			close(l.done)
			return
		}
		backoff.Reset()
		go l.route(conn)
	}
}

// route peeks conn's ClientHello: E2EE connections go to their agent, every
// other one to Accept with the peeked bytes replayed. Without SNI the
// connection passes through too; the TLS handshake there handles or rejects
// it. A share-code SNI is E2EE (<code>.nfltr.xyz, no .e2ee. label).
func (l *muxListener) route(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(session.SNIPeekTimeout))
	sni, peeked, sniErr := session.ExtractSNI(conn)
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		l.logger.Debug("sni-mux: failed to clear read deadline", "error", err)
		_ = conn.Close()
		return
	}
	if sniErr == nil && l.isE2EE(sni) {
		l.routeE2EE(conn, sni, peeked)
		return
	}
	var out net.Conn = conn
	if len(peeked) > 0 {
		out = &session.PrefixConn{Conn: conn, Prefix: peeked}
	}
	select {
	case l.conns <- out:
	case <-l.done:
		_ = conn.Close()
	}
}

// isE2EE determines whether an SNI hostname should be routed as E2EE.
//
// With share grants, the method extracts the subdomain prefix
// (e.g. "<code>" from "<code>.nfltr.xyz") and checks whether it is shaped
// like a share code.  This enables E2EE URLs without a .e2ee. subdomain.
//
// Falls back to static suffix matching without grants.
func (l *muxListener) isE2EE(sni string) bool {
	host := strings.ToLower(sni)

	// Must match the base domain suffix.
	if !strings.HasSuffix(host, l.e2eeSuffix) {
		return false
	}

	// Extract subdomain prefix.
	prefix := strings.TrimSuffix(host, l.e2eeSuffix)
	if prefix == "" {
		return false // bare domain (e.g. "nfltr.xyz") — not E2EE
	}

	// With share grants, a name shaped like a share code is
	// E2EE whatever the store answers: routeE2EE resolves it and refuses
	// it when unknown or when the store fails, so the relay never
	// terminates TLS for a share's name (its certificate may cover it).
	// Other names (e.g. "grpc", "www") are never a share's and stay with
	// the relay's TLS, without a store lookup.
	if l.grants != nil {
		return l.grants.IsCode(prefix)
	}

	// No share grants — fall back to static suffix match (legacy behaviour).
	return true
}

// Close implements net.Listener.
func (l *muxListener) Close() error {
	return l.inner.Close()
}

// Addr implements net.Listener.
func (l *muxListener) Addr() net.Addr {
	return l.inner.Addr()
}

// routeE2EE resolves the SNI hostname and routes the raw TLS connection
// through the TCP dispatcher, after the share's gate (e2ee_share_gate.go).
// The server never terminates TLS for these connections.
func (l *muxListener) routeE2EE(conn net.Conn, sni string, peeked []byte) {
	routeE2EE(l.ctx, l.resolver, l.tcpDispatcher, l.logger, conn, sni, peeked, time.Now())
}
