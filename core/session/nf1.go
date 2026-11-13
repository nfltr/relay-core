// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/nfltr/relay-core/core/directory"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

// NF1NotServed is the one answer to an open of a capability address that
// no single live agent serves here: unknown, offline, declared by two
// agents, or a relay without the shared directory. It names no agent, so a
// caller learns nothing about which rendezvous ids exist (nfltr
// docs/decisions/nf1-rendezvous-2026-11-13.md section 4).
const NF1NotServed = "no nf1 service at this address"

func nf1NotServed() *Refusal {
	return &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, Msg: NF1NotServed}
}

// openNF1 is Open of a capability address: the one live agent whose record
// declares t.NF1 (directory.NF1Index), its service of that declaration,
// end to end encrypted, as an anonymous caller (no identity reaches the
// agent). The agent's owner pays (the frontend meters the session). A
// refusal never names the agent; a DialError does, for the frontend's log
// only.
func (c *Core) openNF1(ctx context.Context, t Target) (Opened, error) {
	idx, ok := c.Dispatcher.IPInventory.(directory.NF1Index)
	if !ok {
		return Opened{}, nf1NotServed()
	}
	budget := c.routeBudget
	if budget == 0 {
		budget = routeBudget
	}
	route, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	agents, err := idx.NF1Agents(route, t.NF1)
	if err != nil {
		return Opened{}, &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, Msg: "the relay's directory is unavailable"}
	}
	if len(agents) != 1 {
		if len(agents) > 1 {
			c.Dispatcher.Logger.Warn("nf1: a rendezvous id declared by more than one agent: not served", "rid", hex.EncodeToString(t.NF1[:]), "agents", agents)
		}
		return Opened{}, nf1NotServed()
	}
	agentID := agents[0]
	services, connected := c.Dispatcher.AgentServices(agentID)
	decl, ok := agentservices.FindNF1(services, t.NF1)
	if !connected || !ok {
		return Opened{}, nf1NotServed()
	}
	release := func() {}
	if t.Admit != nil {
		if release, err = t.Admit(agentID); err != nil {
			return Opened{}, err
		}
	}
	retries, interval := c.DialPolicy()
	ep := TCPEndpoint{AgentID: agentID, TargetAddr: decl.Name, E2EE: true, Answer: t.Answer}
	s, done, err := c.TCPDispatcher.OnwardWithRetry(route, ep, retries, interval)
	if err != nil {
		release()
		return Opened{}, &DialError{AgentID: agentID, TargetAddr: decl.Name, Err: fmt.Errorf("nf1: %w", err)}
	}
	return Opened{Session: s, AgentID: agentID, Release: func() {
		if done != nil {
			done()
		}
		release()
	}}, nil
}
