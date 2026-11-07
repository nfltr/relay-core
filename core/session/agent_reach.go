// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay-core/core/identity"
)

// Action is what a reach decision is asked for.
type Action uint8

// The actions a principal may take on an agent (cedar's Action::"dial",
// Action::"list").
const (
	Dial Action = iota // open bytes or a call to it: connect, E2EE, A2A, browse, WireGuard, a peer replica's session
	List               // see it in a listing (presence, my agents, Fleet)
)

// String is the action as the policy names it.
func (a Action) String() string {
	if a == List {
		return "list"
	}
	return "dial"
}

// Policy decides whether principal p may act on agent, an agent of p's own
// account (Reach checked that first). relay-server's is cedar-go over the
// account's groups and rules, keyed by its revision; relay-core holds none
// of its own. The policy reads p.Machine's group from its own snapshot (a
// move takes effect at the revision that made it), never from p.Group. An error is the policy's unavailability (the caller answers
// it as retryable), never "no".
type Policy interface {
	Allowed(p identity.Principal, agent identity.AgentRef, a Action) (bool, error)
}

// Reach is the one reach decision (relay-server spec/Reach.tla Allowed):
// every dial path and every listing asks it. p reaches agentID when it is
// an agent ref of p's own account (its owner: the principal it connected
// with here, else its directory record) and the Policy allows the action.
// A different account is refused before any policy is asked; no Policy
// refuses everything (fail closed, no legacy pass-through). An owner the
// directory could not read is an error, not "no". Under SingleTenant (DEV
// ONLY) every authenticated principal reaches every agent.
func (d *Dispatcher) Reach(p identity.Principal, agentID string, a Action) (bool, error) {
	if p.Owner == "" && p.Subject == "" {
		return false, nil
	}
	if d.SingleTenant {
		return true, nil
	}
	ref, err := identity.ParseAgentRef(agentID)
	if p.Machine == "" || err != nil {
		return false, nil
	}
	owner, err := d.agentOwner(ref, agentID)
	if err != nil {
		return false, err
	}
	if owner == "" || owner != p.Owner || d.Policy == nil {
		return false, nil
	}
	return d.Policy.Allowed(p, ref, a)
}

// agentOwner is the account of agent ref's machine: MachineOwner's, else
// the directory's record of the agent.
func (d *Dispatcher) agentOwner(ref identity.AgentRef, agentID string) (string, error) {
	if d.MachineOwner != nil {
		if owner, err := d.MachineOwner(ref.Machine); err != nil || owner != "" {
			return owner, err
		}
	}
	return d.OwnerForAgent(agentID)
}

// DialReaches is Reach(p, ·, Dial) for the entry points that open bytes to
// an agent (HTTP /connect and data sessions, the direct agent subdomain,
// WireGuard exits): the relay's in-process agents are never reached this
// way.
func (d *Dispatcher) DialReaches(p identity.Principal) func(agentID string) (bool, error) {
	return func(agentID string) (bool, error) {
		if d.IsInternalAgent(agentID) {
			return false, nil
		}
		return d.Reach(p, agentID, Dial)
	}
}

// agentPrincipal is the principal agent connected with, as far as any
// replica can tell (its machine from the ref, its owner from its
// connection here or its directory record): the source of an A2A call.
// ok is false for an agent with no known owner.
func (d *Dispatcher) agentPrincipal(agentID string) (identity.Principal, bool, error) {
	ref, err := identity.ParseAgentRef(agentID)
	if err != nil {
		return identity.Principal{}, false, nil
	}
	owner, err := d.OwnerForAgent(agentID)
	if err != nil || owner == "" {
		return identity.Principal{}, false, err
	}
	return identity.Principal{Owner: owner, Machine: ref.Machine, Subject: agentID, AgentID: agentID}, true, nil
}

// A2AReaches is Reach for agent-to-agent traffic, which also carries relay
// plumbing: an authenticated agent may reach the relay's internal agents,
// and an internal agent registered in this process may reach any agent.
// The replica that forwards a call and the one that delivers it
// (RelayTunnel) both ask it.
func (d *Dispatcher) A2AReaches(source, target string) (bool, error) {
	if source == "" {
		return false, nil
	}
	if d.SingleTenant {
		return true, nil
	}
	if d.IsInternalAgent(source) || d.IsInternalAgent(target) {
		return true, nil
	}
	p, ok, err := d.agentPrincipal(source)
	if !ok {
		return false, err
	}
	return d.Reach(p, target, Dial)
}

// AccountIsOneGroup is the Policy of a relay without groups (the core's own
// composition, compose.Serve): every machine of an account reaches every
// agent of it. A relay with groups (relay-server) sets its own.
type AccountIsOneGroup struct{}

// Allowed implements Policy: Reach already checked the account.
func (AccountIsOneGroup) Allowed(identity.Principal, identity.AgentRef, Action) (bool, error) {
	return true, nil
}
