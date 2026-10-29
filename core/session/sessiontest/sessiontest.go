// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package sessiontest holds test-only helpers over a session.Dispatcher
// (used by tests), kept out of the production relay binary.
package sessiontest

import (
	"log/slog"
	"slices"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/proto/pb"
)

// AwaitUpstream blocks until agentID's upstream is registered: an agent's
// AgentTunnel call returns before the dispatcher finishes admission, so a
// call routed to it right after can race to "target agent not connected".
// Driven by the registration broadcast (UpstreamRegCond), not a poll.
func AwaitUpstream(d *session.Dispatcher, agentID string) {
	d.UpstreamRegMu.Lock()
	defer d.UpstreamRegMu.Unlock()
	for {
		if _, ok := d.Upstreams.Load(agentID); ok {
			return
		}
		d.UpstreamRegCond.Wait()
	}
}

// HasUpstream reports whether agentID has a registered upstream.
func HasUpstream(d *session.Dispatcher, agentID string) bool {
	_, ok := d.Upstreams.Load(agentID)
	return ok
}

// SetSingleTenant makes every caller one tenant (bare agent ids), as a
// trust_loopback relay does. Call before the dispatcher serves.
func SetSingleTenant(d *session.Dispatcher, v bool) { d.SingleTenant = v }

// FanoutTxn delivers f to the waiters of transaction txn, as an agent's
// response frame does.
func FanoutTxn(d *session.Dispatcher, txn string, f *pb.TunnelFrame, logger *slog.Logger) error {
	return d.TxnRegistry.Fanout(txn, f, logger)
}

// SetSessionStall sets how long a TCP session's frame waits on a stuck agent
// stream before the session is reset (session.TCPSessionStall by default).
func SetSessionStall(d *session.TCPDispatcher, v time.Duration) { d.SessionStall = v }

// AgentSessionCount is the number of sessions on agentID's streams here.
func AgentSessionCount(d *session.TCPDispatcher, agentID string) int {
	d.Mu.RLock()
	streams := slices.Clone(d.All[agentID])
	d.Mu.RUnlock()
	n := 0
	for _, as := range streams {
		as.Mu.Lock()
		n += len(as.Sessions)
		as.Mu.Unlock()
	}
	return n
}

// CloseAgentSessions ends every session on agentID's streams here.
func CloseAgentSessions(d *session.TCPDispatcher, agentID string) {
	d.Mu.RLock()
	streams := slices.Clone(d.All[agentID])
	d.Mu.RUnlock()
	for _, as := range streams {
		as.CloseAll()
	}
}
