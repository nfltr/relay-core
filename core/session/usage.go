// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "github.com/nfltr/relay-core/platform/metrics"

// OpenMeter is the meter of a session on p into agentID: the agent's owner
// pays for it (OwnerForAgent), whoever opened it; an owner not known or not
// readable leaves it to no payer, never drops it.
func (d *Dispatcher) OpenMeter(p metrics.Path, agentID string) *metrics.Meter {
	return d.OpenShareMeter(p, agentID, "")
}

// OpenShareMeter is OpenMeter of a visit through the share whose id is
// share (metrics.OpenShareMeter; "" for none).
func (d *Dispatcher) OpenShareMeter(p metrics.Path, agentID, share string) *metrics.Meter {
	if d == nil {
		return openMeter(p, agentID, share, nil)
	}
	return openMeter(p, agentID, share, d.OwnerForAgent)
}

// OpenMeter is Dispatcher.OpenMeter for this dispatcher's sessions (no
// payer before WireTCP joins it to the dispatcher).
func (t *TCPDispatcher) OpenMeter(p metrics.Path, agentID string) *metrics.Meter {
	return t.OpenShareMeter(p, agentID, "")
}

// OpenShareMeter is Dispatcher.OpenShareMeter for this dispatcher's sessions.
func (t *TCPDispatcher) OpenShareMeter(p metrics.Path, agentID, share string) *metrics.Meter {
	return openMeter(p, agentID, share, t.ownerOf)
}

func openMeter(p metrics.Path, agentID, share string, ownerOf func(string) (string, error)) *metrics.Meter {
	owner := ""
	if ownerOf != nil && metrics.UsageEnabled() {
		owner, _ = ownerOf(agentID)
	}
	return metrics.OpenShareMeter(p, owner, agentID, share)
}
