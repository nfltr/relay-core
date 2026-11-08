// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"

	"github.com/nfltr/relay-core/core/relaystatus"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// An agent with several relays holds one link to each (nfltr
// docs/decisions/relay-failover-2026-11-08.md, spec/Failover.tla). Each of
// its connects names the relays it already holds links to
// (agentwire.AgentRelaysKey); an endpoint of a named relay refuses before
// it claims anything, so the agent never holds two links to one relay and
// its live link's record stays (claim-before-dedupe: a claim first would
// supersede it). Only the agent knows which links it holds: the relay
// cannot tell a live old session from a stale one, and failover needs the
// newest claim to win.

// refuseAttached is the connect of kind (ConnectHTTP, AgentTunnel,
// ConnectTCP) refused ALREADY_ATTACHED when its metadata names this
// relay's id, nil otherwise (no RelayID: never). Every connect calls it
// before it takes a session over or claims the agent's record.
func (d *Dispatcher) refuseAttached(ctx context.Context, agentID, kind string) error {
	named := metadata.ValueFromIncomingContext(ctx, agentwire.AgentRelaysKey)
	if !agentwire.NamesRelay(named, d.RelayID) {
		return nil
	}
	traceFailover(d, ctx, "refuse", agentID, kind)
	d.Logger.Info("connect names this relay among the agent's: refused, already attached", "agent_id", agentID, "kind", kind, "relay_id", d.RelayID)
	return relaystatus.New(codes.AlreadyExists, relayv1.ErrorReason_ERROR_REASON_ALREADY_ATTACHED,
		"the agent already holds a link to this relay ("+d.RelayID+") through another endpoint").Err()
}
