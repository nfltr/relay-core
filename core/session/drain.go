// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"math/rand/v2" // nosemgrep: math-random-used -- drain jitter, not a secret
	"time"

	"github.com/nfltr/relay-core/core/directory"
)

// drainJitter is when, after the drain starts, an agent that does not move
// is let go.
func drainJitter(window time.Duration) time.Duration {
	if window <= 1 {
		return 0
	}
	return rand.N(window / 2) // #nosec G404 -- jitter, not a secret
}

// ConfigureTCPRenew makes t renew a TCP agent's streamer inventory record
// (written once by ConnectTCP with the heartbeat threshold as TTL) while its
// stream lives; an agent whose AgentTunnel is here is left to that tunnel,
// which renews the record with its labels. A renewal that finds a newer
// session of the agent on another replica ends the stream
// (directory.ErrSuperseded).
func (d *Dispatcher) ConfigureTCPRenew(t *TCPDispatcher) {
	ttl := d.Config.HeartbeatRecvThreshold
	if d.StreamerIPInventory == nil || ttl <= 0 {
		return
	}
	t.ConfigureRenew(ttl/3, func(agentID string, s directory.Session) error {
		if _, ok := d.Upstreams.Load(agentID); ok {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), ttl/3)
		defer cancel()
		err := renewAgentRecord(ctx, d.StreamerIPInventory, agentID, s, nil, nil, ttl)
		switch {
		case errors.Is(err, directory.ErrSuperseded):
			d.Logger.Info("TCP agent reconnected to another pod: ending its old stream here", "agent_id", agentID)
		case err != nil:
			d.RecordWriteFailed.Store(true) // written again at the next epoch tick (inventory_epoch.go)
			d.Logger.Debug("renewing a TCP agent's streamer record", "agent_id", agentID, "error", err)
		}
		return err
	}, d.reannounced)
}

// UnmovedStreams is how many agents' TCP streams stay routed here through
// a drain: those of agents that do not move. The TCP stream of an agent
// that moves stays until the agent closes it with its old connection (its
// open sessions run on it): it moves with the agent's HTTP session.
func (d *TCPDispatcher) UnmovedStreams() int {
	n := 0
	d.Mu.RLock()
	for _, as := range d.Streams {
		if !as.moves {
			n++
		}
	}
	d.Mu.RUnlock()
	return n
}
