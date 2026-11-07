// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"strings"

	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// AgentAddress is an agent as a caller names it (relay-server
// spec/Reach.tla Dial, DialOwn; nfltr
// docs/decisions/tenants-reach-policy-2026-11-07.md "Names are labels"),
// one of:
//
//   - a ref "<machine id>-<service>" (Ref set): that agent;
//   - a bare service "<service>" (Own): the caller's own machine's service,
//     never another machine's (no store is read: `nfltr dial db` is this
//     machine's db);
//   - "<machine name>.<service>" (Machine set): the service of the
//     machine of the caller's account holding that name, resolved once at
//     the edge (a '.' is in neither label, and a '/' cannot go in a
//     /connect path segment).
//
// A2A routing takes refs only: the caller's E2EE prologue binds the
// callee ref it states and the callee checks it against its own ref, so a
// name the relay resolved would be a relay-told identity the callee
// refuses.
type AgentAddress struct {
	Ref     identity.AgentRef
	Machine string // a machine name
	Service string
	Own     bool
}

// ParseAgentAddress is the one parser of an agent address; ok is false
// for a string that is none of the forms.
func ParseAgentAddress(s string) (AgentAddress, bool) {
	if ref, err := identity.ParseAgentRef(s); err == nil {
		return AgentAddress{Ref: ref, Service: ref.Service}, true
	}
	name, svc, dotted := strings.Cut(s, ".")
	switch {
	case !dotted && identity.ValidService(s):
		return AgentAddress{Service: s, Own: true}, true
	case dotted && identity.ValidMachineName(name) && identity.ValidService(svc):
		return AgentAddress{Machine: name, Service: svc}, true
	}
	return AgentAddress{}, false
}

// NameResolver is a Policy that also resolves machine names: the machine
// of p's account named name at the policy's revision ("" none). An error
// is the store's unavailability, never "no such name".
type NameResolver interface {
	ResolveMachine(p identity.Principal, name string) (identity.MachineID, error)
}

// ResolveAgent is the edge's one name resolution: the agent ref address
// names for caller p. A ref is itself; a bare service is p's own
// machine's; a machine name is looked up in p's account only (the
// Policy's NameResolver), an unknown name refused as a missing agent. A
// caller with no machine (a dashboard session) names refs only: its
// address is returned as given for Reach to decide.
func (d *Dispatcher) ResolveAgent(p identity.Principal, address string) (string, *Refusal) {
	a, ok := ParseAgentAddress(address)
	switch {
	case !ok || a.Ref.Machine != "":
		return address, nil
	case a.Own:
		if p.Machine == "" {
			return address, nil
		}
		return identity.AgentRef{Machine: p.Machine, Service: a.Service}.String(), nil
	}
	r, ok := d.Policy.(NameResolver)
	if !ok || p.Owner == "" {
		return "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, Msg: "agent not found: this relay resolves no machine names; name the agent by its ref", AgentID: address}
	}
	m, err := r.ResolveMachine(p, a.Machine)
	switch {
	case err != nil:
		return "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, Msg: "the relay's directory is unavailable", AgentID: address}
	case m == "":
		return "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, Msg: "agent not found: no machine " + a.Machine + " in this account", AgentID: address}
	}
	return identity.AgentRef{Machine: m, Service: a.Service}.String(), nil
}
