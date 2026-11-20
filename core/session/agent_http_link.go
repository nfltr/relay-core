// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"net"

	"github.com/nfltr/relay/proto/wire/dataplane"
)

// httpSessionsOnly is the HTTPSessionsHeader value of an agent that serves
// HTTP sessions and no raw TCP (relay client agent.HTTPSessionsOnly): it is
// not listed as a TCP agent.
const httpSessionsOnly = "only"

// httpSessions is what an agent's ConnectTCP request metadata announces of
// HTTP sessions: "" none, "1", or httpSessionsOnly.
func httpSessions(ctx context.Context) string {
	if v := firstIncoming(ctx, dataplane.HTTPSessionsHeader); v == "1" || v == httpSessionsOnly {
		return v
	}
	return ""
}

// HTTPRoute names the route of agentID's HTTP sessions here: its newest
// primary stream's number (no other primary of this relay has it), 0 when
// that primary serves no HTTP sessions or the agent has none here. A newer
// primary changes it: the HTTP pool's key (spec/HTTPPool.tla Key).
func (d *TCPDispatcher) HTTPRoute(agentID string) uint64 {
	d.Mu.RLock()
	defer d.Mu.RUnlock()
	if as := d.Streams[agentID]; as != nil && as.http != "" {
		return as.route
	}
	return 0
}

// DialHTTP opens an HTTP session to agentID. Its Route() is the route it
// rides: the primary it was placed on (HTTPRoute's number then), 0 when
// the agent has no stream here (the session goes to its replica).
func (d *TCPDispatcher) DialHTTP(ctx context.Context, agentID string) (net.Conn, error) {
	ep := TCPEndpoint{AgentID: agentID, TargetAddr: dataplane.HTTPSessionTarget}
	as := d.agentStream(agentID)
	if as == nil {
		return d.DialAgent(ctx, agentID, ep.TargetAddr)
	}
	s, release, err := d.onwardLocal(ctx, ep, as)
	if err != nil {
		return nil, err
	}
	c := newDialedSession(ctx, s, release)
	c.route = primaryOf(as).route
	return c, nil
}
