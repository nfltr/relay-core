// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"

	"github.com/nfltr/relay-core/core/directory"
)

// Make-before-break move of an agent off a leaving replica (pb.Move). Letting an agent go during a drain fails the visits this
// replica had already routed to its stream (≈15 per replaced replica at 52
// agents × 1 visit/s). An agent that advertised agentwire.MoveKey is asked
// instead, as the drain starts, to connect again (MOVE_RECONNECT): the new
// connection reaches another replica, whose session takes the agent's
// record (the newest session wins). Once the agent says the new connection
// is up (MOVE_CONNECTED) and the record names the newer session, this
// replica stops routing to the old stream (requests resolve to the new
// replica through the record), waits for the requests the ingress already
// picked it for (their route leases, RouteHere), and its consumer sends
// MOVE_DONE after every request it holds (spec/Move.tla): every request
// the old stream carries precedes MOVE_DONE. The agent closes the old
// stream once it has answered every request it received there, and the
// session ends. A renewal of the record that finds the new session's after
// a rebalancing move gave up, or a late MOVE_CONNECTED, goes on with the
// move (spec/README.md finding 7).
// An agent that does not report back within its moveBound is let go as before;
// an agent that predates the move never gets MOVE_RECONNECT. Its TCP stream
// (same agent, same flag) is not let go at all during a drain: the agent
// closes it with its old connection once its sessions ended.
//
// Only a replica with peers moves agents (the pod dialer): a single relay
// has nowhere to send them. A leaving replica refuses new agent sessions,
// so a reconnect cannot land on it.
const moveCheck = 50 * time.Millisecond

// defaultMoveBound is how long an agent asked to move has to report its new
// connection before it is let go (Dispatcher.moveBound: tests shorten it
// per dispatcher, never package-wide, since a session watcher of another
// test may still read it).
const defaultMoveBound = 10 * time.Second

// advertisesMove reports an agent stream whose metadata says it moves.
func advertisesMove(ctx context.Context) bool {
	return firstIncoming(ctx, agentwire.MoveKey) == "1"
}

// Leaving reports a replica with peers that is shutting down.
func (d *Dispatcher) Leaving() bool {
	return d.PodDialer != nil && d.globalContext != nil && d.globalContext.Err() != nil
}

// agentMove is one HTTP session's move, driven by watchAgentSession.
type agentMove struct {
	stream *HTTPStream
	check  *time.Ticker // until the record names the new session
}

// routeLeases counts the requests the ingress picked a stream for whose
// first message is not yet on the consumer's channel (spec/Move.tla
// Pick, Publish). Once stopped it gives no new lease, and idle closes when
// the last one ends.
type routeLeases struct {
	mu   sync.Mutex
	off  bool
	n    int
	idle chan struct{}
}

func (r *routeLeases) take() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.off {
		return false
	}
	r.n++
	return true
}

func (r *routeLeases) give() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.n--; r.off && r.n == 0 {
		close(r.idle)
	}
}

// stop gives no new lease; the channel closes once none is held.
func (r *routeLeases) stop() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.off {
		r.off, r.idle = true, make(chan struct{})
		if r.n == 0 {
			close(r.idle)
		}
	}
	return r.idle
}

// RouteHere takes a route lease on agentID's HTTP session here: ok when the
// agent is connected here and this replica still routes to it. release
// (idempotent) ends the lease once the request's first message is
// published to the agent's topic, or failed: until then a move does not
// tell the agent that no request follows (MOVE_DONE).
func (d *Dispatcher) RouteHere(agentID string) (release func(), ok bool) {
	s, ok := d.Agents.Load(agentID)
	if !ok || !s.leases.take() {
		return nil, false
	}
	return sync.OnceFunc(s.leases.give), true
}

// moves reports a session this replica moves rather than lets go: an HTTP
// session of an agent that moves, on a replica with peers and a session
// store.
func (d *Dispatcher) moves(tracker HeartbeatTracker, inv directory.IPInventory) (*HTTPStream, bool) {
	s, ok := tracker.(*HTTPStream)
	_, store := inv.(directory.SessionStore)
	return s, ok && store && s.moved != nil && d.PodDialer != nil
}

// startMove sends MOVE_RECONNECT to an agent that moves; nil when it does
// not (the caller lets it go).
func (d *Dispatcher) startMove(tracker HeartbeatTracker, inv directory.IPInventory) *agentMove {
	s, ok := d.moves(tracker, inv)
	if !ok {
		return nil
	}
	s.moving.Store(true)
	if err := s.Send(&pb.HTTPProxyRequest{Move: pb.Move_MOVE_RECONNECT}); err != nil {
		return nil
	}
	d.Logger.Info("asked the agent to connect again before it leaves this replica", "AgentID", s.ID)
	return &agentMove{stream: s}
}

// connected starts checking the agent's record once it reported its new
// connection; it returns the check's channel.
func (m *agentMove) connected() <-chan time.Time {
	m.check = time.NewTicker(moveCheck)
	return m.check.C
}

// recordMoved reports the agent's record naming another session than this
// one (its new connection's, on another replica). Not "a newer one": the
// sessions' start times come from two replicas' clocks, and a skewed clock
// must not keep an agent from moving.
func (d *Dispatcher) recordMoved(ctx context.Context, inv directory.IPInventory, m *agentMove) bool {
	_, s, err := inv.(directory.SessionStore).GetSession(ctx, m.stream.ID)
	own := m.stream.Session()
	return err == nil && (!s.Since.Equal(own.Since) || s.Credential != own.Credential)
}

// stopRouting takes the old stream out of this replica's routing (requests
// now resolve to the new replica); the channel closes once no route lease
// is held.
func (d *Dispatcher) stopRouting(m *agentMove) <-chan struct{} {
	m.stop()
	d.Agents.CompareAndDelete(m.stream.ID, m.stream)
	d.movedAway.Store(m.stream.ID, m.stream) // until its session ends (dispatcher_subscribe.go)
	return m.stream.leases.stop()
}

// done has the session's consumer tell the agent, after every request it
// holds, that no new request follows on the old stream (MOVE_DONE).
func (m *agentMove) done() { close(m.stream.moveDone) }

func (m *agentMove) stop() {
	if m.check != nil {
		m.check.Stop()
	}
}

// noteMoveConnected records the agent's MOVE_CONNECTED.
func (s *Stream[S, R]) noteMoveConnected() {
	if s.moved != nil {
		s.movedOnce.Do(func() { close(s.moved) })
	}
}

// ReplicaMover is a multi-replica relay's Mover: it moves the agents of d.
type ReplicaMover struct {
	D *Dispatcher
}

// Move asks agentID's session here to move to another replica; false when
// it cannot move now (gone, moving already, too young, or carrying TCP
// sessions).
func (m *ReplicaMover) Move(agentID string) bool { return m.D.moveAgent(agentID) }

// Rebalance moves agents off this replica toward an even share of total
// agents over replicas and returns how many it asked to move.
func (m *ReplicaMover) Rebalance(total, replicas int) int {
	return m.D.rebalanceAgents(total, replicas)
}

// ReannounceAll wakes every agent session here to rewrite its directory
// record now.
func (m *ReplicaMover) ReannounceAll() { m.D.reannounceAll() }
