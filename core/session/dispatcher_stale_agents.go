// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// RunStaleAgentReaper periodically evicts gRPC upstream agents that have not
// sent inbound messages within staleAgentMaxIdle. It ticks only while an
// agent other than the relay's own in-process ones is connected (a gRPC
// agent's registration wakes it): an idle relay's reaper holds no timer.
func (d *Dispatcher) RunStaleAgentReaper(ctx context.Context) {
	var ticker *time.Ticker
	var tick <-chan time.Time
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		switch connected := d.hasExternalUpstream(); {
		case connected && ticker == nil:
			ticker = time.NewTicker(d.staleAgentReaperInterval)
			tick = ticker.C
		case !connected && ticker != nil:
			ticker.Stop()
			ticker, tick = nil, nil
		}
		select {
		case <-ctx.Done():
			return
		case <-d.staleReaperWake:
		case <-tick:
			d.reapStaleAgents()
		}
	}
}

// wakeStaleAgentReaper tells RunStaleAgentReaper an agent connected.
func (d *Dispatcher) wakeStaleAgentReaper() {
	select {
	case d.staleReaperWake <- struct{}{}:
	default: // a wake is pending
	}
}

// hasExternalUpstream reports an upstream agent RegisterInternalAgent did
// not register.
func (d *Dispatcher) hasExternalUpstream() bool {
	found := false
	d.Upstreams.Range(func(agentID string, _ Streamer) bool {
		_, internal := d.internalAgents.Load(agentID)
		found = !internal
		return !found
	})
	return found
}

// reapStaleAgents evicts agents idle past staleAgentMaxIdle. In-process
// agents (RegisterInternalAgent) live as long as the process and send
// nothing while idle: they are never reaped.
func (d *Dispatcher) reapStaleAgents() {
	now := time.Now()
	d.Upstreams.Range(func(agentID string, _ Streamer) bool {
		if _, internal := d.internalAgents.Load(agentID); internal {
			return true
		}
		lastSeen, ok := d.AgentLastSeen(agentID)
		if !ok || d.TCPDispatcher.LinkUp(agentID) { // its agent link's liveness vouches for it
			return true
		}
		idleFor := now.Sub(lastSeen)
		if idleFor <= d.staleAgentMaxIdle {
			return true
		}
		d.Logger.Warn("stale agent reaper: evicting idle agent", "agent_id", agentID, "idle_for", idleFor)
		d.disconnectAgent(agentID)
		return true
	})
}

// disconnectAgent evicts a gRPC upstream agent and runs the same disconnect
// hooks as a normal stream teardown.
func (d *Dispatcher) disconnectAgent(agentID string) {
	d.lastSeenByAgentID.Delete(agentID)
	d.InventoryRefreshByAgentID.Delete(agentID)
	if cancel, ok := d.agentTunnelCancels.LoadAndDelete(agentID); ok {
		cancel.(*agentTunnelCancel).cancel(nil)
		return
	}
	stream, ok := d.Upstreams.Load(agentID)
	if !ok {
		return
	}
	grpcStream, ok := stream.(*GRPCStream)
	if !ok {
		if d.Upstreams.CompareAndDelete(agentID, stream) {
			d.NumberOfAgentsConnected.Add(-1)
			d.forgetAgent(agentID)
		}
		return
	}
	if !d.DeregisterUpstreamAgent(agentID, grpcStream, errors.New("stale agent reaper eviction")) {
		return
	}
	if err := d.StreamerIPInventory.DeleteIPs(context.Background(), agentID); err != nil {
		d.Logger.Error("failed to delete agentID dispatcher addr from cache", "AgentID", agentID, "error", err)
	}
	d.publishDeparture(agentID, grpcStream)
}

// touchAgentLastSeen records inbound activity for the stale-agent reaper.
func (d *Dispatcher) touchAgentLastSeen(agentID string) {
	if d == nil || agentID == "" {
		return
	}
	d.agentSeenAt(agentID, time.Now())
}

// seenEpoch is what lastSeenByAgentID's times count from: a time.Since of
// it keeps the monotonic clock (a wall-clock step moves no agent's idle
// time).
var seenEpoch = time.Now()

// agentSeenAt records agentID's last inbound activity, t. The agent's
// entry is an *atomic.Int64 (t - seenEpoch) set in place: storing a
// time.Time per inbound frame allocated a map entry and the boxed time on
// every frame (docs/performance.md, R3).
func (d *Dispatcher) agentSeenAt(agentID string, t time.Time) {
	n := int64(t.Sub(seenEpoch))
	if v, ok := d.lastSeenByAgentID.Load(agentID); ok {
		v.(*atomic.Int64).Store(n)
		return
	}
	v := new(atomic.Int64)
	v.Store(n)
	if old, loaded := d.lastSeenByAgentID.LoadOrStore(agentID, v); loaded {
		old.(*atomic.Int64).Store(n)
	}
}
