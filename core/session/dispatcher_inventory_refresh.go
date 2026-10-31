// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/metrics"
)

func (d *Dispatcher) inboundInventoryRefreshInterval() time.Duration {
	recvThreshold := d.Config.HeartbeatRecvThreshold
	if recvThreshold <= 0 {
		return 0
	}
	interval := recvThreshold / 2
	if interval <= 0 {
		return recvThreshold
	}
	return interval
}

func streamLabels(tracker HeartbeatTracker) map[string]string {
	if labeled, ok := tracker.(interface{ Labels() map[string]string }); ok {
		return labeled.Labels()
	}
	return nil
}

// streamSession is the session a tracker's stream is (zero when unknown).
func streamSession(tracker HeartbeatTracker) directory.Session {
	if s, ok := tracker.(interface{ Session() directory.Session }); ok {
		return s.Session()
	}
	return directory.Session{}
}

// claimAgentRecord writes the agent's record for this pod's new session s
// and returns s with its place in the agent's session order (Seq); an
// inventory without sessions (no shared store) as before.
func claimAgentRecord(ctx context.Context, inv directory.IPInventory, agentID string, s directory.Session, labels map[string]string, capabilities []string, ttl time.Duration) (directory.Session, error) {
	if store, ok := inv.(directory.SessionStore); ok {
		return store.ClaimSession(ctx, agentID, s, labels, capabilities, ttl)
	}
	return s, inv.SetIPWithAgentInfo(ctx, agentID, labels, capabilities, ttl)
}

// renewAgentRecord rewrites the agent's record for session s, or returns
// directory.ErrSuperseded when a newer session of the agent on another pod
// holds it (directory.SessionStore); s zero or no session store: as before.
func renewAgentRecord(ctx context.Context, inv directory.IPInventory, agentID string, s directory.Session, labels map[string]string, capabilities []string, ttl time.Duration) error {
	if store, ok := inv.(directory.SessionStore); ok && !s.Since.IsZero() {
		return store.PutSession(ctx, agentID, s, true, labels, capabilities, ttl)
	}
	return inv.SetIPWithAgentInfo(ctx, agentID, labels, capabilities, ttl)
}

func streamCapabilities(tracker HeartbeatTracker) []string {
	if capable, ok := tracker.(interface{ Capabilities() []string }); ok {
		return capable.Capabilities()
	}
	return nil
}

// refreshAgentInventoryOnInboundActivity renews the cross-pod agent inventory
// TTL from any inbound stream message, not just explicit heartbeat frames.
// The first inbound message after connect refreshes immediately; subsequent
// busy-stream messages are throttled so Redis is not updated per frame. It
// returns directory.ErrSuperseded, writing nothing, when a newer session of
// the agent (s) on another pod holds the record: the caller ends this one.
func (d *Dispatcher) refreshAgentInventoryOnInboundActivity(ctx context.Context, agentID string, inv directory.IPInventory, s directory.Session, labels map[string]string, capabilities []string) error {
	if d == nil || agentID == "" || inv == nil {
		return nil
	}
	if d.IsInternalAgent(agentID) {
		// Every replica runs its own (the relay's agent): no session of it
		// supersedes another's, each renews its record as before.
		s = directory.Session{}
	}
	if ctx != nil && ctx.Err() != nil {
		// The session ended (a frame its receive loop drains): record
		// nothing that would outlive it (forgetAgent).
		return nil
	}
	d.touchAgentLastSeen(agentID)
	recvThreshold := d.Config.HeartbeatRecvThreshold
	refreshInterval := d.inboundInventoryRefreshInterval()
	if recvThreshold <= 0 || refreshInterval <= 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return nil
	}

	now := time.Now()
	if lastRaw, ok := d.InventoryRefreshByAgentID.Load(agentID); ok {
		if last, ok := lastRaw.(time.Time); ok && now.Sub(last) < refreshInterval {
			return nil
		}
	}
	d.InventoryRefreshByAgentID.Store(agentID, now)

	err := renewAgentRecord(ctx, inv, agentID, s, labels, capabilities, recvThreshold)
	if errors.Is(err, directory.ErrSuperseded) {
		d.Logger.Info("agent reconnected to another pod: ending its old session here", "AgentID", agentID)
		return err
	}
	if err != nil {
		if d.Logger != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				d.Logger.Debug("ipInventory.SetIP on inbound activity skipped after stream context ended", "AgentID", agentID, "error", err)
			} else {
				d.Logger.Warn("ipInventory.SetIP on inbound activity failed (best-effort, agent stream preserved)", "AgentID", agentID, "error", err)
			}
		}
		if ctx.Err() == nil {
			// Written again at the next epoch tick, not a refresh interval
			// later (inventory_epoch.go).
			d.InventoryRefreshByAgentID.Delete(agentID)
			d.RecordWriteFailed.Store(true)
		}
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			metrics.RegisterUpstreamHeartbeatFailure("setip_warn")
		}
	}
	return nil
}

func (d *Dispatcher) agentInventoryRegistrationContext(streamCtx context.Context) context.Context {
	if d != nil && d.globalContext != nil && d.globalContext.Err() == nil {
		return d.globalContext
	}
	if streamCtx != nil {
		return streamCtx
	}
	return context.Background()
}
