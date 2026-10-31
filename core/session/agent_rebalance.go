// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"time"
)

// Rebalancing agents over the replicas.
// Agents stay on the replica they connected to, so after a scale-up or a
// replica restart a new replica gets none of the existing agents. Each
// replica compares the agents it holds with its share of the fleet's
// (the ceiling of the mean, from the counts every replica writes to the
// shared directory on its epoch tick, inventory_epoch.go in the relay);
// above its share it asks at most rebalanceMovesPerTick of its excess agents
// per tick to move with the drain's make-before-break move (agent_move.go),
// and refuses moving and reconnecting agents until it holds its share, so a
// moved agent does not land on it (no ping-pong); new agents are admitted.
// An agent whose new connection fails stays (no break) and none of its
// sessions is asked again for stayFor. Only an agent that advertises the
// move is asked.

// defaultRebalanceMovesPerTick bounds the moves a replica asks for per
// epoch tick (5 s): 2 reconnects a second per replica, so a scale-up never
// causes a reconnect storm (Dispatcher.rebalanceMovesPerTick: tests change
// it per dispatcher).
const defaultRebalanceMovesPerTick = 10

// stayFor is how long an agent whose rebalancing move reached no other
// replica is left alone: none of its sessions is asked to move, and its
// reconnects are admitted above the share. An agent that dials one replica's
// address (no load balancer: a compose stack naming one replica) lands
// every try of its move back here, refused; asking each of its sessions
// again cost 40 refused connections (5/s for the client's 8 s) per session,
// and refusing its reconnects kept it off until its record expired
// (TestRebalanceAgentReachingOneReplicaStays). Through a load balancer a
// move that reached no other replica is rare (40 tries, each on a replica
// above its share).
const stayFor = time.Hour

// defaultRebalanceMinAge is how long an agent's session runs here before it
// may be asked to move: one that just arrived (a drain's or another
// replica's move) is not moved on before the fleet's counts show it (kind
// soak 2026-10-21, rollout: agents moved twice within seconds).
// Dispatcher.rebalanceMinAge: tests change it per dispatcher.
const defaultRebalanceMinAge = 30 * time.Second

// AgentsHere counts the agents' HTTP sessions here, one moving away
// included until its new replica routes to it (a move counts on both
// replicas for a moment, never on none: a total that is low would make
// replicas shed agents they then take back).
func (d *Dispatcher) AgentsHere() int {
	n := 0
	d.Agents.Range(func(string, *HTTPStream) bool {
		n++
		return true
	})
	return n
}

// rebalanceAgents takes the fleet's agents (total) over its replicas: above its
// share this replica refuses moved agents and asks up to
// rebalanceMovesPerTick of its excess agents, less those already moving,
// to move. It returns how many it asked. A replica without peers, alone,
// or leaving (its drain moves every agent) does nothing.
func (d *Dispatcher) rebalanceAgents(total, replicas int) int {
	// Every tick, also alone (a scale-down), so stays are bounded by stayFor.
	d.stays.Range(func(id, at any) bool {
		if time.Since(at.(time.Time)) >= stayFor {
			d.stays.CompareAndDelete(id, at)
		}
		return true
	})
	if d.PodDialer == nil || replicas < 2 || d.Leaving() {
		d.overShare.Store(false)
		return 0
	}
	share := (total + replicas - 1) / replicas
	here, moving := 0, 0
	d.Agents.Range(func(_ string, s *HTTPStream) bool {
		here++
		if s.moving.Load() {
			moving++
		}
		return true
	})
	d.overShare.Store(here > share)
	excess := min(here-moving-share, d.rebalanceMovesPerTick)
	asked := 0
	d.Agents.Range(func(id string, _ *HTTPStream) bool {
		if asked >= excess {
			return false
		}
		if d.moveAgent(id) {
			asked++
		}
		return true
	})
	if asked > 0 {
		d.Logger.Info("rebalancing: this replica holds more than its share of the fleet's agents; asking some to move",
			"agents_here", here, "moving", moving, "share", share, "replicas", replicas, "asked", asked)
	}
	return asked
}

// moveAgent asks agentID's session here to move (ReplicaMover.Move); false
// when it has gone, cannot move, is moving already, runs too young,
// reached no other replica last time (stays) or carries TCP sessions: the
// agent closes a connection it moved off once its sessions ended, at most
// 30 s later (relay client moveTCPBound), which cuts a longer one, and a
// rebalancing move is never worth that (kind soak 2026-10-26: held
// sessions cut ConnLost 30 s after their agent's rebalancing move).
func (d *Dispatcher) moveAgent(agentID string) bool {
	s, ok := d.Agents.Load(agentID)
	if !ok || s.rebalance == nil || time.Since(s.ConnectedAt()) < d.rebalanceMinAge || d.staysHere(agentID) {
		return false
	}
	if t := d.TCPDispatcher; t != nil && t.carriesSessions(agentID) {
		return false
	}
	if !s.rebalanceAsked.CompareAndSwap(false, true) || !s.moving.CompareAndSwap(false, true) {
		return false
	}
	close(s.rebalance) // its WatchAgentSession starts the move
	return true
}

// refusesAgents reports a replica that takes no session of agentID now:
// leaving, moving that agent away (its new connection must reach another
// replica), still ending its moved session (its subscription is held
// until then: TestConnectWhileTheMovedSessionStillEndsIsUnavailable), or
// above its share while the agent's record is held (a move or
// a reconnect, which clients try again), unless the agent stays (its move
// reached no other replica: refused, it would only wait for its record to
// expire). An agent without a record is new:
// admitted, since a client's first connection does not retry a refusal
// (kind soak 2026-10-21: a refused first connection ended the agent).
func (d *Dispatcher) refusesAgents(ctx context.Context, agentID string) bool {
	if d.Leaving() {
		return true
	}
	if s, ok := d.Agents.Load(agentID); ok && s.moving.Load() {
		return true
	}
	if _, ok := d.movedAway.Load(agentID); ok {
		return true
	}
	if !d.overShare.Load() || d.staysHere(agentID) {
		return false
	}
	ip, err := d.IPInventory.GetIP(ctx, agentID)
	return err == nil && ip != ""
}

// staysHere reports an agent whose rebalancing move reached no other
// replica within the last stayFor.
func (d *Dispatcher) staysHere(agentID string) bool {
	at, ok := d.stays.Load(agentID)
	return ok && time.Since(at.(time.Time)) < stayFor
}

// giveUpMove ends a rebalancing move whose agent did not report a new
// connection within moveBound: it stays here, renewing its record again.
func (m *agentMove) giveUp() {
	m.stop()
	m.stream.moving.Store(false)
}

// tcpMovingAway flags the agent's TCP stream here (if any) as moving away
// with a rebalancing move, or not once the move is given up.
func (d *Dispatcher) tcpMovingAway(agentID string, on bool) {
	t := d.TCPDispatcher
	if t == nil {
		return
	}
	t.Mu.RLock()
	as := t.Streams[agentID]
	t.Mu.RUnlock()
	if as != nil {
		as.movingAway.Store(on)
	}
}
