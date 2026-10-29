// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errSessionReplaced is the cancel cause of a tunnel session that a newer
// connection with the same agent ID and the same API key took over.
var errSessionReplaced = errors.New("session replaced by a newer connection of this agent")

// sessionTakeoverWait bounds how long a reconnecting session waits for the
// session it replaces to finish its teardown (registry, inventory and broker
// subscription) before it registers itself.
var sessionTakeoverWait = 10 * time.Second

// sessionTakeover is what a live ConnectHTTP session exposes so a reconnect
// of the same agent can replace it: the key it connected with, a function
// that ends it with errSessionReplaced, and a channel closed once its
// teardown is complete.
type sessionTakeover struct {
	keyHash string
	replace func()
	ended   chan struct{}
}

// takeOverSession ends this pod's live session of agentID, if any, and
// waits for its teardown, so a reconnect (after a network drop the relay has
// not noticed yet) replaces its own previous session instead of being
// refused. Only the same agent ID on the same API key (the same principal)
// may take over; anything else is refused. The replaced client receives
// errReplacedSessionStatus, which clients treat as final, so two processes that
// share a name cannot evict each other forever. Its TCP stream stays: the
// newest one takes every new session (TCPDispatcher.agentStream), and the
// replaced client closes its connection once the sessions it carries ended
// (relay client v0.19.4+, bounded at 30 s; a rolling update loses no open
// stream). When the new
// connection reached another replica, this one's renewal is superseded and
// the replaced stream leaves routing but stays for its open sessions
// (TCPDispatcher.yield, TestTCPTakeoverOnAnotherReplicaLetsOpenSessionsFinish).
func (d *Dispatcher) takeOverSession(ctx context.Context, agentID, apiKeyHash string) error {
	old, live := d.Agents.Load(agentID)
	if !live {
		return nil
	}
	t := old.takeover
	if t == nil || t.keyHash != apiKeyHash {
		d.Logger.Info("agent already connected to this pod with another credential", "AgentID", agentID)
		return status.Errorf(codes.AlreadyExists, "agent %s is already connected with another credential; stop that session or pick another --name", agentID)
	}
	d.Logger.Info("reconnecting agent takes over its previous session", "AgentID", agentID)
	t.replace()
	timer := time.NewTimer(sessionTakeoverWait)
	defer timer.Stop()
	select {
	case <-t.ended:
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-timer.C:
		return status.Errorf(codes.Unavailable, "the previous session of agent %s is still closing; retry", agentID)
	}
}

// agentTunnelCancel ends one AgentTunnel session (Dispatcher.agentTunnelCancels
// holds the live one per agent ID). A newer connection of the same ID from
// the same process instance (agentInstanceHeader: a reconnect) ends the
// older with errSessionReconnected, a retryable Canceled nobody acts on;
// from another instance (or one that sends none) with errSessionReplaced,
// so that client learns it was replaced (rpcclient.IsSessionReplaced, final).
// A pointer, so the ending session removes only its own entry.
type agentTunnelCancel struct {
	cancel   context.CancelCauseFunc
	instance string
}

// agentInstanceHeader is rpcclient.InstanceHeader: the connecting process's
// instance id.
const agentInstanceHeader = "x-agent-instance"

// errSessionReconnected ends an AgentTunnel stream its own process replaced
// with a newer connection.
var errSessionReconnected = errors.New("stream superseded by this agent's own reconnect")

// errReplacedSessionStatus is what the replaced session's client receives.
// It names no agent ID: clients classify unknown codes by message text.
var errReplacedSessionStatus = status.Error(codes.FailedPrecondition, "this session was replaced by a newer one with the same agent ID and key")
