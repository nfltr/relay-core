// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package frontend is the contract between the relay's core and its
// frontends: each maps an outside protocol (WireGuard,
// SNI E2EE passthrough, HTTP ingress) onto the core's sessions. A frontend
// is a package under frontend/ that imports the core and shared leaf
// utilities, never another frontend or the composition (the relay
// package); a binary links the frontends it attaches.
package frontend

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/nfltr/relay-core/core/session"
)

// Frontend is one frontend as a binary links it.
type Frontend interface {
	// Name names the frontend, unique per binary.
	Name() string
	// Settings are the frontend's settings, under its own prefix; a
	// binary that does not link the frontend has none of them.
	Settings() []Setting
	// Attach joins the frontend to a relay before it serves; ctx ends
	// with the relay's run. A frontend its configuration leaves off
	// attaches nothing.
	Attach(ctx context.Context, h Host) error
}

// Setting is one of a frontend's settings: a flag of the relay (and its
// environment variable and config key, as every relay setting).
type Setting struct {
	Name, Usage string
	// Default is the setting's type and default: bool, string or
	// time.Duration.
	Default any
}

// Host is what a frontend attaches to: one replica's core and the hosted
// product's checks, as the composition (CoreHost, or the binary's own) provides them.
type Host interface {
	// Core is the replica's session core.
	Core() *session.Core
	Logger() *slog.Logger
	// Visible is principal's tenant scope over agent IDs (the scope its
	// dashboard lists); nil when the relay authenticates no one, every
	// agent then.
	Visible(principal string) func(agentID string) bool
	// RecordWireGuardKey keeps an agent's WireGuard public key in the
	// endpoint registry, which the dashboard and proof page show.
	RecordWireGuardKey(agentID, publicKey string)

	// WrapGRPCListener puts wrap in front of the gRPC port's TLS: wrap
	// gets the raw listener, the relay applies TLS to what it returns.
	// False (and wrap unused) when the relay applies no TLS there.
	WrapGRPCListener(wrap func(raw net.Listener) net.Listener) bool
	// Listener is the listener the relay bound for a port config key
	// before it wrote its ports file, nil when it bound none: a frontend
	// serving a port of its own serves it, so a port configured as "0" is
	// the kernel-chosen one the ports file reports.
	Listener(key string) net.Listener
	// Go runs fn with the relay's listeners: an error ends the relay.
	Go(fn func() error)
	// Grants verify the relay's share codes; nil when it has no shares.
	Grants() GrantVerifier

	// MountHTTP mounts HTTP ingress: the relay's HTTP server hands it the
	// requests its routing sends to an agent, the replica link the
	// requests peers forward.
	MountHTTP(AgentHTTP)
	// Registered reports whether agentID is a registered endpoint of the
	// endpoint registry: known, though maybe not connected.
	Registered(ctx context.Context, agentID string) bool
	// RelayCookie reports whether name is one of the relay's own browser
	// cookies, which never reach an agent.
	RelayCookie(name string) bool
}

// AgentHTTP is HTTP ingress as the relay's servers mount it.
type AgentHTTP struct {
	// Serve serves a request the relay's routing sends to an agent
	// (session.AgentIDKey names it).
	Serve http.Handler
	// Peer serves a request a peer replica forwarded over the replica
	// link.
	Peer http.Handler
	// Fanout hands agents' ConnectHTTP answers to the requests waiting for
	// them until ctx ends; the relay runs it with its gRPC server.
	Fanout func(ctx context.Context) error
	// AgentGone releases what the ingress keeps for an agent whose
	// session ended.
	AgentGone func(agentID string)
}

// Grant is what a share code lets an anonymous visitor reach.
type Grant struct {
	// AgentID is the share's agent; Service one of its services, "" for
	// the whole agent.
	AgentID, Service string
	// Policy is the share's access policy, its secrets sealed.
	Policy SharePolicy
}

// SharePolicy is a share's access policy. Restrictions are ANDed; an empty
// policy admits everyone. Secrets (BasicAuth "user:password",
// BearerToken, RequiredHeaders' values) are sealed by the issuer: a
// verifier never holds them in plaintext.
type SharePolicy struct {
	BasicAuth       string            `json:"basic_auth,omitempty"`
	AllowedIPs      []string          `json:"allowed_ips,omitempty"`
	BearerToken     string            `json:"bearer_token,omitempty"`
	RequiredHeaders map[string]string `json:"required_headers,omitempty"`
	// Preset is the issuer's workflow name; ExpiresAt the share's expiry
	// (RFC 3339, "" when unknown).
	Preset    string `json:"preset,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

// Gated is true when the share has credentials only the agent can check
// on an E2EE session (Basic Auth, a bearer token, required headers): the
// relay hands those sessions only to an agent stream that enforces them.
func (p SharePolicy) Gated() bool {
	return p.BasicAuth != "" || p.BearerToken != "" || len(p.RequiredHeaders) > 0
}

// Admits reports whether the share admits a client at ip: its IP
// allowlist (CIDR ranges or exact addresses) holds ip, or it has none.
func (p SharePolicy) Admits(ip net.IP) bool {
	if len(p.AllowedIPs) == 0 {
		return true
	}
	if ip == nil {
		return false
	}
	for _, entry := range p.AllowedIPs {
		entry = strings.TrimSpace(entry)
		if _, ipNet, err := net.ParseCIDR(entry); err == nil {
			if ipNet.Contains(ip) {
				return true
			}
			continue
		}
		if allowed := net.ParseIP(entry); allowed != nil && allowed.Equal(ip) {
			return true
		}
	}
	return false
}

// ErrNoGrant answers a code no share has.
var ErrNoGrant = errors.New("no share by that code")

// GrantVerifier is all the relay's routing needs of the shares: whether a
// name is shaped like a share code, and what a code grants. Every share
// request (HTTP on a share host, an E2EE connection on the SNI port) is
// routed through it; issuing, listing and revoking shares are not the
// routing's.
type GrantVerifier interface {
	// IsCode reports whether s has the shape of a share code (no lookup).
	IsCode(s string) bool
	// Grant looks code up and keeps it reserved: ErrNoGrant when no share
	// has it; any other error is the verifier's (its store unreachable).
	Grant(code string) (Grant, error)
}
