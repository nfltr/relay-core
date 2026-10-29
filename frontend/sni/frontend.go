// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package sni is the SNI E2EE passthrough frontend: a share's E2EE URL,
// <share code>.<base domain>, reaches the share's agent with the visitor's
// TLS unopened. A listener peeks each connection's ClientHello for its
// server name, maps it through the share grants to the agent, and hands the
// raw bytes to the core's TCP sessions; the agent terminates TLS. It runs
// in front of the relay's TLS on the gRPC port (Mux), or on a port of its
// own when the relay applies no TLS there (Router).
package sni

import (
	"context"
	"fmt"
	"net"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/spf13/viper"
)

// Frontend is SNI E2EE passthrough as a binary links it. Its settings
// (e2ee.tls_passthrough.*) are the relay's own: the HTTP ingress names
// share URLs by the base domain too.
type Frontend struct{}

// Name implements frontend.Frontend.
func (Frontend) Name() string { return "sni" }

// Settings implements frontend.Frontend: none of its own.
func (Frontend) Settings() []frontend.Setting { return nil }

// Attach implements frontend.Frontend: with tcp sessions and
// e2ee.tls_passthrough.enabled and a base domain, E2EE connections are
// routed in front of the gRPC port's TLS, or, when the relay applies no TLS
// there, on e2ee.tls_passthrough.port (the listener the relay bound for it).
func (Frontend) Attach(ctx context.Context, h frontend.Host) error {
	d := h.Core().TCPDispatcher
	baseDomain := viper.GetString("e2ee.tls_passthrough.base_domain")
	if d == nil || !viper.GetBool("e2ee.tls_passthrough.enabled") || baseDomain == "" {
		return nil
	}
	logger := h.Logger()
	grants := h.Grants()
	if h.WrapGRPCListener(func(inner net.Listener) net.Listener {
		logger.Info("E2EE SNI mux enabled on gRPC port", "base_domain", baseDomain, "addr", inner.Addr())
		return NewMux(ctx, inner, baseDomain, grants, d, logger.With("subsystem", "sni-mux"))
	}) {
		return nil
	}
	ln := h.Listener(config.SNIRouterPort) // bound by the relay's Listen, "0" kernel-chosen
	if ln == nil {
		return fmt.Errorf("e2ee sni router: the relay bound no listener for %s", config.SNIRouterPort)
	}
	logger.Info("E2EE TLS passthrough enabled", "addr", ln.Addr(), "base_domain", baseDomain)
	router := NewRouter(baseDomain, grants, d, logger.With("subsystem", "sni-router"))
	h.Go(func() error { return router.Serve(ctx, ln) })
	return nil
}
