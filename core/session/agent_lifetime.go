// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay/proto/wire/agentwire"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// admitAgentLifetime validates the connect labels' lifetime declaration and
// reports whether this connection is ephemeral. An id that already holds a
// durable registration (a named tunnel, a long-lived worker) stays durable:
// the declaration is dropped from labels so the row is never marked
// ephemeral. labels is the stream's freshly parsed map and may be modified.
func (d *Dispatcher) admitAgentLifetime(agentID string, labels map[string]string) (bool, error) {
	v, declared := labels[agentwire.LifetimeLabel]
	if !declared {
		return false, nil
	}
	if v != agentwire.LifetimeEphemeral {
		return false, status.Errorf(codes.InvalidArgument, "label %s=%q is not a lifetime; the only declarable lifetime is %q (omit the label for a durable registration)",
			agentwire.LifetimeLabel, v, agentwire.LifetimeEphemeral)
	}
	if d.DurableRegistration != nil && d.DurableRegistration(agentID) {
		d.Logger.Warn("ephemeral lifetime refused: the agent id holds a durable registration; connecting as durable", "AgentID", agentID)
		delete(labels, agentwire.LifetimeLabel)
		return false, nil
	}
	return true, nil
}

// removeEphemeralRegistration removes agentID's registry row unless a
// reconnect of the same id has already registered a new stream.
// ponytail: one lock for every ephemeral id; per-id locks if connect churn
// ever contends on it.
func (d *Dispatcher) removeEphemeralRegistration(agentID string) {
	if d.OnEphemeralAgentGone == nil {
		return
	}
	d.ephemeralMu.Lock()
	defer d.ephemeralMu.Unlock()
	if _, back := d.Upstreams.Load(agentID); back {
		return
	}
	d.OnEphemeralAgentGone(agentID)
}
