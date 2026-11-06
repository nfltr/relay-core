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
	"github.com/nfltr/relay-core/core/relaystatus"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/tracehook"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
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
	ing.proxy.dispatcher = c.Dispatcher
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

// peer serves a request a peer replica forwarded over the replica link (it
// read the agent's record naming this replica): it takes the route lease
// as dispatch does, so a move's MOVE_DONE waits for its first message.
// Refused (this replica no longer routes to the agent), it is not yet at
// any agent: the record names this replica (the agent's session here
// registers in a moment: it waits, finding 45, Move.tla ArriveB), or
// another replica, the agent's after a move (it goes on there, once:
// finding 46, Move.tla ArriveA, MovedForwards), else it is answered 503
// Retry-After before it reaches any agent, which would run it after
// MOVE_DONE with its answer lost (spec/README.md finding 20).
func (ing *Ingress) peer(w http.ResponseWriter, r *http.Request) {
	agentID := r.Header.Get(session.AgentIDKey)
	wentOn := r.Header.Get(peerHopHeader) != ""
	r.Header.Del(peerHopHeader)
	release, ok := tracehook.RoutePeer(ing.core.Dispatcher, agentID, ing.core.Dispatcher.RouteHere)
	if !ok {
		ip, err := ing.core.DispatcherIPInventory.GetIP(r.Context(), agentID)
		switch {
		case err != nil || ip == "":
		case ip == ing.core.Dispatcher.Config.LoopbackAddr:
			release, ok = ing.core.Dispatcher.RouteArriving(r.Context(), agentID)
		case !wentOn:
			if u, err := ing.peerURL(ip); err == nil {
				r.Header.Set(peerHopHeader, "1")
				ing.peers.Get(u).ServeHTTP(w, r)
				return
			}
		}
	}
	if !ok {
		w.Header().Set("Retry-After", "1")
		relaystatus.WriteHTTP(w, http.StatusServiceUnavailable, codes.Unavailable, relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, agentOfflineMessage)
		return
	}
	defer release()
	ing.proxy.ServeHTTP(w, r.WithContext(withRouteLease(r.Context(), release)))
}

// Mount is the ingress as the relay mounts it.
func (ing *Ingress) Mount() frontend.AgentHTTP {
	return frontend.AgentHTTP{
		Serve: ing.front(HeaderRewriteMiddleware(ing.policy, http.HandlerFunc(ing.dispatch))),
		Peer:  http.HandlerFunc(ing.peer),
		Fanout: func(ctx context.Context) error {
			return ing.proxy.FanoutResponses(ctx, ing.core.PodID, ing.core.PodIDTopicSubscriber)
		},
		AgentGone: ing.proxy.producers.Remove,
	}
}

// peerHopHeader marks a peer's forward this replica sent on to the replica
// the agent's record names (Ingress.peer): that replica serves it or
// refuses it, never forwards it again.
const peerHopHeader = "X-Nfltr-Peer-Hop"
