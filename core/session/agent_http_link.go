// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"

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

// ServesHTTP reports whether agentID's primary TCP stream on this replica
// serves HTTP sessions.
func (d *TCPDispatcher) ServesHTTP(agentID string) bool {
	d.Mu.RLock()
	defer d.Mu.RUnlock()
	as := d.Streams[agentID]
	return as != nil && as.http != ""
}
