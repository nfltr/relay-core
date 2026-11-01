// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "github.com/nfltr/relay-core/platform/metrics"

// OpenMeter is the meter of a session on p into agentID: the agent's owner
// pays for it (OwnerForAgent), whoever opened it; an owner not known or not
// readable leaves it to no payer, never drops it.
func (d *Dispatcher) OpenMeter(p metrics.Path, agentID string) *metrics.Meter {
	if d == nil {
		return openMeter(p, agentID, nil)
	}
	return openMeter(p, agentID, d.OwnerForAgent)
}

// OpenMeter is Dispatcher.OpenMeter for this dispatcher's sessions (no
// payer before WireTCP joins it to the dispatcher).
func (t *TCPDispatcher) OpenMeter(p metrics.Path, agentID string) *metrics.Meter {
	return openMeter(p, agentID, t.ownerOf)
}

func openMeter(p metrics.Path, agentID string, ownerOf func(string) (string, error)) *metrics.Meter {
	owner := ""
	if ownerOf != nil && metrics.UsageEnabled() {
		owner, _ = ownerOf(agentID)
	}
	return metrics.OpenMeter(p, owner, agentID)
}
