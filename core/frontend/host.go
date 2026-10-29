// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
)

// CoreHost is a Host over the session core alone (nfltr-relay-core): no
// hosted product behind it, so shares only as the issuer's verifier
// (Shares, nil: none), no endpoint
// registry (Registered false), no relay cookies, nowhere to keep a
// WireGuard key, and no TLS of its own on the gRPC port. The composition
// fills its fields, attaches its frontends (Attach), then serves what they
// mounted (HTTP).
type CoreHost struct {
	C   *session.Core
	Log *slog.Logger
	// Ports are the listeners the composition bound, by port config key.
	Ports map[string]net.Listener
	// Run runs a frontend's server with the composition's listeners: an
	// error ends the composition (errgroup.Group.Go).
	Run func(fn func() error)
	// Shares verifies share codes another process issued (nil: no shares).
	Shares GrantVerifier

	mu   sync.Mutex
	http AgentHTTP
}

var _ Host = (*CoreHost)(nil)

// Attach attaches frontends in order.
func (h *CoreHost) Attach(ctx context.Context, frontends ...Frontend) error {
	for _, f := range frontends {
		if err := f.Attach(ctx, h); err != nil {
			return err
		}
	}
	return nil
}

// HTTP is the HTTP ingress a frontend mounted; zero when none did.
func (h *CoreHost) HTTP() AgentHTTP {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.http
}

// Core returns the host's session core.
func (h *CoreHost) Core() *session.Core { return h.C }

// Logger returns the host's logger.
func (h *CoreHost) Logger() *slog.Logger { return h.Log }

// Visible is the key base's own agents, the rule the core's sessions
// reach by (Dispatcher.PrincipalReaches): the WireGuard frontend's exits.
// An owner the directory cannot read is out of scope.
func (h *CoreHost) Visible(principal string) func(agentID string) bool {
	reaches := h.C.Dispatcher.PrincipalReaches(identity.Principal{Subject: principal})
	return func(agentID string) bool {
		ok, _ := reaches(agentID)
		return ok
	}
}

// RecordWireGuardKey does nothing: the core keeps no endpoint registry.
func (h *CoreHost) RecordWireGuardKey(string, string) {}

// WrapGRPCListener returns false: the core puts nothing in front of the
// gRPC port's TLS.
func (h *CoreHost) WrapGRPCListener(func(net.Listener) net.Listener) bool { return false }

// Listener returns the listener Ports holds for key, nil when none.
func (h *CoreHost) Listener(key string) net.Listener { return h.Ports[key] }

// Go runs fn with Run.
func (h *CoreHost) Go(fn func() error) { h.Run(fn) }

// Grants returns Shares.
func (h *CoreHost) Grants() GrantVerifier { return h.Shares }

// MountHTTP keeps a as the host's HTTP ingress (HTTP).
func (h *CoreHost) MountHTTP(a AgentHTTP) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.http = a
}

// Registered returns false: the core keeps no endpoint registry.
func (h *CoreHost) Registered(context.Context, string) bool { return false }

// RelayCookie returns false: the core sets no browser cookies.
func (h *CoreHost) RelayCookie(string) bool { return false }
