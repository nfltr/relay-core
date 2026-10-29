// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// An agent session's housekeeping, common to HTTP and gRPC agent sessions,
// in one select on the session handler's own goroutine. It was four
// goroutines per session (a receive-end forwarder, the heartbeat monitor,
// the inventory refresh and the relay-stop jitter), each a few KiB of stack
// held for every idle agent, then one beside a handler that only waited.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/platform/metrics"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// runSession runs the session's housekeeping (watchAgentSession) on the
// calling handler goroutine, which would otherwise only wait for eg, and
// then waits for eg: one goroutine (its stack) less per agent. It returns
// what eg.Wait would have returned had the housekeeping run in eg: a
// housekeeping error that comes first ends the group (stop, the cause) and
// is returned; when a group member failed first, that member's error.
// received carries the end of the session's receive loop, which runs
// outside the group: Recv returns only when the stream ends, and the stream
// ends only when the handler returns.
func (d *Dispatcher) runSession(eg *errgroup.Group, ctx context.Context, stop func(cause error), agentID string, tracker HeartbeatTracker, ipInventory directory.IPInventory, received <-chan error) error {
	err := d.WatchAgentSession(ctx, agentID, tracker, ipInventory, received)
	first := err != nil && ctx.Err() == nil
	if first {
		stop(err)
	}
	groupErr := eg.Wait()
	if err != nil && (first || groupErr == nil || errors.Is(groupErr, context.Canceled)) {
		return err
	}
	return groupErr
}

// WatchAgentSession returns when the session must end: ctx is done, the
// receive loop ended (its error from received), no heartbeat arrived within
// the receive threshold (when heartbeats are on: errNoHeartbeat, and the
// agent's inventory entry is removed), or the relay is stopping (an agent
// that moves is asked to at once; another gets errServerRestarting after a
// random jitter up to half the graceful shutdown timeout, so the fleet's
// reconnects spread out). Until then it renews the
// agent's inventory entry every half receive threshold, also while the
// stream is idle.
func (d *Dispatcher) WatchAgentSession(ctx context.Context, agentID string, tracker HeartbeatTracker, inv directory.IPInventory, received <-chan error) error {
	var refresh, refreshEvery <-chan time.Time // refreshEvery: restored when a rebalancing move is given up
	var reannounce <-chan struct{}
	if interval := d.inboundInventoryRefreshInterval(); agentID != "" && tracker != nil && inv != nil && d.Config.HeartbeatRecvThreshold > 0 && interval > 0 {
		reannounce = d.reannounced()
		if err := d.refreshAgentInventoryOnInboundActivity(ctx, agentID, inv, streamSession(tracker), streamLabels(tracker), streamCapabilities(tracker)); err != nil {
			return errSessionReplaced
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		refresh, refreshEvery = ticker.C, ticker.C
	}
	var heartbeats <-chan proto.Message
	var linkVouched bool // the agent link was up at the last receive-threshold tick
	var recvTimeout *time.Ticker
	var recvTimeoutC <-chan time.Time
	if d.Config.HeartbeatEnabled {
		heartbeats, recvTimeout = tracker.Heartbeats(), tracker.HeartbeatsRecvThresholdTicker()
		recvTimeoutC = recvTimeout.C
	}
	var stopping <-chan struct{}
	switch {
	case d.AgentsGo != nil:
		stopping = d.AgentsGo
	case d.globalContext != nil:
		stopping = d.globalContext.Done()
	}
	var stopNow <-chan time.Time
	// A move (agent_move.go): moved until the agent reports its new
	// connection, check until its record names it, settle until MOVE_DONE.
	var mv *agentMove
	var moved <-chan struct{}
	var check, settle <-chan time.Time
	// A rebalancing move (agent_rebalance.go): ask until this replica asks
	// the agent to move; giveUp until its new connection is up and its
	// record names it (then the move goes on as above).
	var ask <-chan struct{}
	if s, ok := d.moves(tracker, inv); ok {
		ask = s.rebalance
	}
	var giveUp <-chan time.Time
	defer func() {
		if mv != nil {
			mv.stop()
		}
	}()
	for {
		select {
		case <-ctx.Done():
			if stopNow != nil {
				return errServerRestarting
			}
			return ctx.Err()
		case err := <-received:
			if err != nil {
				d.logStreamLifecycle("receiving from agent stream", err, "AgentID", agentID)
			}
			return err
		case <-heartbeats:
			resetRecvTimeout(recvTimeout, d.Config.HeartbeatRecvThreshold)
		case <-recvTimeoutC:
			// The agent link's liveness stands for the heartbeats: a link
			// that goes silent ends the agent's streams itself. One that
			// vouched at the last tick and is down now went down within
			// this threshold: the agent answers heartbeats again from its
			// end (a heartbeat interval later), so it gets a whole
			// threshold to (TestHeartbeatThresholdRestartsWhenTheLinkGoesDown).
			up := d.TCPDispatcher.LinkUp(agentID)
			if up || linkVouched {
				linkVouched = up
				resetRecvTimeout(recvTimeout, d.Config.HeartbeatRecvThreshold)
				continue
			}
			err := fmt.Errorf("agentID %s: %w", agentID, errNoHeartbeat)
			d.Logger.Error("heartbeat timeout", "error", err)
			metrics.RegisterUpstreamHeartbeatFailure("recv_timeout")
			if cacheErr := inv.DeleteIPs(context.Background(), agentID); cacheErr != nil {
				d.Logger.Error("failed to delete agentID dispatcher addr from cache", "AgentID", agentID, "error", cacheErr)
			}
			d.logStreamLifecycle("monitoring heartbeats from agent stream", err, "AgentID", agentID)
			return err
		case <-reannounce: // the store lost the records: rewrite now (gates lifted)
			reannounce = d.reannounced()
			if err := d.refreshAgentInventoryOnInboundActivity(ctx, agentID, inv, streamSession(tracker), streamLabels(tracker), streamCapabilities(tracker)); err != nil {
				return errSessionReplaced // directory.ErrSuperseded
			}
		case <-refresh:
			if err := d.refreshAgentInventoryOnInboundActivity(ctx, agentID, inv, streamSession(tracker), streamLabels(tracker), streamCapabilities(tracker)); err != nil {
				return errSessionReplaced // directory.ErrSuperseded
			}
		case <-stopping:
			stopping = nil
			if mv != nil { // a rebalancing move: the drain bounds it from now on
				giveUp = nil
				timer := time.NewTimer(d.moveBound)
				defer timer.Stop()
				stopNow = timer.C
				continue
			}
			td := drainJitter(d.gracefulShutdownTimeout) // drain.go
			if _, moves := d.moves(tracker, inv); moves || d.AgentsGo != nil {
				td = 0 // it moves at once (agent_move.go); or no peers to spread the agents over
			}
			d.Logger.Info("waiting before disconnecting agent", "duration", td, "AgentID", agentID)
			timer := time.NewTimer(td)
			defer timer.Stop()
			stopNow = timer.C
		case <-stopNow:
			if mv == nil {
				if mv = d.startMove(tracker, inv); mv != nil {
					refresh, reannounce = nil, nil // the new session takes the record
					moved = mv.stream.moved
					timer := time.NewTimer(d.moveBound)
					defer timer.Stop()
					stopNow = timer.C
					continue
				}
			}
			d.logStreamLifecycle("handling stop signal", errServerRestarting, "AgentID", agentID)
			return errServerRestarting
		case <-ask:
			ask = nil
			if mv != nil || stopNow != nil { // the drain moves it
				continue
			}
			if mv = d.startMove(tracker, inv); mv == nil {
				tracker.(*HTTPStream).moving.Store(false) // Rebalance set it; the session is ending
			} else {
				refresh, reannounce = nil, nil // the new session takes the record
				d.tcpMovingAway(agentID, true)
				moved = mv.stream.moved
				timer := time.NewTimer(d.moveBound)
				defer timer.Stop()
				giveUp = timer.C
			}
		case <-giveUp: // no new connection elsewhere: the agent stays, renewing its record
			giveUp, moved, check = nil, nil, nil
			mv.giveUp()
			d.tcpMovingAway(agentID, false)
			d.stays.Store(agentID, time.Now()) // its sessions are not asked again (stayFor)
			mv = nil
			refresh = refreshEvery
			if refreshEvery != nil {
				reannounce = d.reannounced()
			}
			d.Logger.Info("rebalancing: the agent did not connect to another replica; it stays here", "AgentID", agentID)
		case <-moved:
			moved = nil
			check = mv.connected()
		case <-check:
			if d.recordMoved(ctx, inv, mv) {
				check, giveUp = nil, nil
				settle = d.stopRouting(mv)
			}
		case <-settle:
			settle, stopNow = nil, nil // the agent closes the stream; the forced stop bounds it
			if err := mv.done(); err != nil {
				return err
			}
			d.Logger.Info("agent moved to another replica: closing its old stream here", "AgentID", agentID)
		}
	}
}

// resetRecvTimeout stops the ticker, drains any tick that was already queued
// in its channel before the reset took effect (time.Ticker.Reset documents
// that it does not drain the channel), then reschedules. This eliminates a
// race where receiveloop's onRecv callback and this goroutine both Reset the
// same ticker: without the drain, a stale tick can fire a false-positive
// `recv_timeout` and kill the session even though the agent is sending
// heartbeats every 15s.
func resetRecvTimeout(ticker *time.Ticker, threshold time.Duration) {
	ticker.Stop()
	select {
	case <-ticker.C:
	default:
	}
	ticker.Reset(threshold)
}
