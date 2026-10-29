// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package httpingress is HTTP ingress: a request the relay's routing names
// an agent for (a share, a browse view, a site host, an endpoint route, a
// key's own request) reaches that agent's HTTP. In process when the agent is
// connected to this replica: on an HTTP session over its link (client
// generation 3) or as ConnectHTTP messages through the in-process broker
// (older agents); else over the replica link to the replica holding it.
// The relay's HTTP server routes and authenticates; this package carries
// the request.
package httpingress

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/spf13/viper"
)

// Frontend is HTTP ingress as a binary links it. Its settings (http.*,
// proxy.*) are the relay's own: the relay's HTTP server reads the same
// keys.
type Frontend struct{}

// Name implements frontend.Frontend.
func (Frontend) Name() string { return "http" }

// Settings implements frontend.Frontend: none of its own.
func (Frontend) Settings() []frontend.Setting { return nil }

// Attach implements frontend.Frontend: it mounts the ingress on the
// relay's HTTP server and replica link.
func (Frontend) Attach(ctx context.Context, h frontend.Host) error {
	h.MountHTTP(New(ctx, h).Mount())
	return nil
}

// Ingress carries requests to agents for one replica.
type Ingress struct {
	core   *session.Core
	logger *slog.Logger
	// registered is the host's endpoint registry check (a known agent
	// that is offline answers 503).
	registered func(ctx context.Context, agentID string) bool

	proxy  *Proxy      // ConnectHTTP, through the in-process broker
	link   *agentHTTP  // HTTP sessions; nil without TCP sessions
	peers  *proxyCache // the peer replicas
	policy *HeaderPolicy

	peerLinkPort string
	// apiTypes is http.api_type_prefixes, read once (apiType).
	apiTypes []string
	// loopbackURL names this replica: compared by identity, never dialled.
	loopbackURL *url.URL
}

// New makes the ingress over h's core, once its dispatchers are wired;
// what it runs in the background (the proxy's transaction reaper) ends with
// ctx, the relay's run.
func New(ctx context.Context, h frontend.Host) *Ingress {
	c, logger := h.Core(), h.Logger()
	ing := &Ingress{
		core:         c,
		logger:       logger,
		registered:   h.Registered,
		proxy:        NewProxy(ctx, logger.With("type", "proxy", "pod_id", c.PodID), c.Broker, h.RelayCookie),
		peers:        newProxyCache(logger),
		policy:       NewHeaderPolicyFromConfig(logger),
		peerLinkPort: viper.GetString("peer_link.port"),
		apiTypes:     viper.GetStringSlice("http.api_type_prefixes"),
		loopbackURL:  &url.URL{Scheme: "http", Host: "in-process"},
	}
	if c.TCPDispatcher != nil {
		ing.link = newAgentHTTP(c.TCPDispatcher, logger.With("subsystem", "agent-http"), h.RelayCookie)
	}
	// A replica that has peers forwards over the replica link only (nil
	// links: refused, session.ErrPeerLinkOff).
	if d := c.Dispatcher; d != nil && d.PodDialer != nil {
		if tr, ok := ing.peers.transport.(*http.Transport); ok {
			if c.PeerLinks != nil {
				tr.DialContext = c.PeerLinks.HTTPDial(ing.peerLinkPort, tr.DialContext)
			} else {
				tr.DialContext = session.LinkOffHTTPDial(ing.peerLinkPort, tr.DialContext)
			}
		}
	}
	return ing
}

// Mount is the ingress as the relay mounts it.
func (ing *Ingress) Mount() frontend.AgentHTTP {
	return frontend.AgentHTTP{
		Serve: ing.front(HeaderRewriteMiddleware(ing.policy, http.HandlerFunc(ing.dispatch))),
		Peer:  ing.proxy,
		Fanout: func(ctx context.Context) error {
			return ing.proxy.FanoutResponses(ctx, ing.core.PodID, ing.core.PodIDTopicSubscriber)
		},
		AgentGone: ing.proxy.producers.Remove,
	}
}
