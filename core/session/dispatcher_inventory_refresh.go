// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"sync"
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

// streamRecordLock orders a stream's directory record writes (nil when the
// tracker has none).
func streamRecordLock(tracker HeartbeatTracker) *sync.Mutex {
	if l, ok := tracker.(interface{ recordLock() *sync.Mutex }); ok {
		return l.recordLock()
	}
	return nil
}

// streamRecordStale is the stream's stale-record flag, read and written
// under streamRecordLock (nil when the tracker has none).
func streamRecordStale(tracker HeartbeatTracker) *bool {
	if l, ok := tracker.(interface{ staleRecord() *bool }); ok {
		return l.staleRecord()
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

// releaseAgentRecord removes the agent's record when it names this pod's
// session s (the shared inventory's DeleteSession), so a stream that ends
// after a newer session of the agent claimed the record leaves it; an
// inventory without sessions: by this pod (DeleteIPs).
func releaseAgentRecord(ctx context.Context, inv directory.IPInventory, agentID string, s directory.Session) error {
	if store, ok := inv.(interface {
		DeleteSession(context.Context, string, directory.Session) error
	}); ok {
		return store.DeleteSession(ctx, agentID, s)
	}
	return inv.DeleteIPs(ctx, agentID)
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
func (d *Dispatcher) refreshAgentInventoryOnInboundActivity(ctx context.Context, agentID string, inv directory.IPInventory, tracker HeartbeatTracker) error {
	if d == nil || agentID == "" || inv == nil {
		return nil
	}
	s := streamSession(tracker)
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

	// The labels are read under the stream's record lock, as a label
	// update writes them: a renewal never writes labels an update replaced
	// (spec/README.md finding 10).
	if lock := streamRecordLock(tracker); lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	labels, caps := streamLabels(tracker), streamCapabilities(tracker)
	err := renewAgentRecord(ctx, inv, agentID, s, labels, caps, recvThreshold)
	if traceOn && inv == d.StreamerIPInventory {
		switch {
		case err == nil:
			tracePresRecord(d, agentID, "Renew", true, labels)
		case errors.Is(err, directory.ErrSuperseded):
			tracePresRecord(d, agentID, "Superseded", true, nil)
		}
	}
	stale := streamRecordStale(tracker) // under the record lock
	if err == nil && stale != nil && *stale {
		// The other replicas listed what the failed write or the lost
		// record left: wake their watchers (spec/README.md finding 27).
		*stale = false
		d.Presence.up(agentID, labels, caps, d.presenceServices(agentID))
	}
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
			if stale != nil {
				*stale = true
			}
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

// rewriteRecord renews the stream's record when the store may have lost it
// (reannounceAll): the renewal that writes it publishes (finding 27).
func (d *Dispatcher) rewriteRecord(ctx context.Context, agentID string, inv directory.IPInventory, tracker HeartbeatTracker) error {
	if lock, stale := streamRecordLock(tracker), streamRecordStale(tracker); lock != nil && stale != nil {
		lock.Lock()
		*stale = true
		lock.Unlock()
	}
	return d.refreshAgentInventoryOnInboundActivity(ctx, agentID, inv, tracker)
}

// departed releases the agent's record for session s, then publishes its
// down. A release that failed leaves the record, listed by every replica
// until its TTL, whose expiry publishes nothing: a second down once the TTL
// passed wakes their watchers to re-list it gone (spec/README.md finding 27).
func (d *Dispatcher) departed(agentID string, s directory.Session, stream Streamer) {
	err := releaseAgentRecord(context.Background(), d.StreamerIPInventory, agentID, s)
	if traceOn {
		tracePresRecord(d, agentID, "End", err == nil, nil)
	}
	if err != nil {
		d.Logger.Error("failed to delete agentID dispatcher addr from cache", "AgentID", agentID, "error", err)
		d.lateDown(agentID, stream)
	} else {
		d.Logger.Debug("deleted agentID dispatcher addr from cache", "AgentID", agentID)
	}
	d.publishDeparture(agentID, stream)
}

// lateDown publishes the agent's down again once the record a failed
// release left has passed its TTL (Presence.tla LateDown, findings 27, 29).
func (d *Dispatcher) lateDown(agentID string, stream Streamer) {
	if ttl := d.Config.HeartbeatRecvThreshold; ttl > 0 {
		time.AfterFunc(ttl+time.Second, func() {
			if traceOn {
				tracePresRecord(d, agentID, "LateDown", true, nil)
			}
			d.publishDeparture(agentID, stream)
		})
	}
}
