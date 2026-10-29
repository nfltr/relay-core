// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"strings"

	"github.com/nfltr/relay-core/core/identity"
)

// AgentCredentialReaches is the one ownership rule for a caller holding an
// agent credential, shared by the HTTP and gRPC entry points. caller is the
// credential's key base (its agent ID when it has no key), callerOwner the
// account owning it ("" none). The caller reaches agentID when it is the
// caller, an agent under "<caller>.", or an agent whose owner (ownerOf) is
// callerOwner. No caller reaches nothing; an unowned caller reaches only
// its own namespace.
func AgentCredentialReaches(caller, callerOwner, agentID string, ownerOf func(string) string) bool {
	if caller == "" || agentID == "" {
		return false
	}
	if agentID == caller || strings.HasPrefix(agentID, caller+".") {
		return true
	}
	return callerOwner != "" && ownerOf != nil && ownerOf(agentID) == callerOwner
}

// agentReaches applies agentCredentialReaches to a gRPC caller (its agent
// ID and key base).
func (d *Dispatcher) agentReaches(caller, base, agentID string) (bool, error) {
	return d.reachScope(caller, base)(agentID)
}

// ownerReaches is AgentCredentialReaches with the owners OwnerForAgent
// reads: false with the directory's error (directory.ErrUnavailable) when
// an owner it needed could not be read, so a caller answers the relay's
// unavailability, never "not found". callerOwnerErr is the error reading
// the caller's own owner.
func (d *Dispatcher) ownerReaches(caller, callerOwner string, callerOwnerErr error, agentID string) (bool, error) {
	var failed error
	ok := AgentCredentialReaches(caller, callerOwner, agentID, func(id string) string {
		owner, err := d.OwnerForAgent(id)
		if err != nil {
			failed = err
		}
		return owner
	})
	switch {
	case ok:
		return true, nil
	case failed != nil:
		return false, failed
	}
	return false, callerOwnerErr
}

// reachScope is agentReaches for one caller over many agents: the caller's
// owner (its principal's, ownerForAgent) is resolved once. Under
// security.trust_loopback (DEV ONLY) every authenticated caller is the one
// local tenant, as in agentTenantScope.
func (d *Dispatcher) reachScope(caller, base string) func(agentID string) (bool, error) {
	if caller == "" {
		return func(string) (bool, error) { return false, nil }
	}
	if d.SingleTenant {
		return func(string) (bool, error) { return true, nil }
	}
	if base == "" {
		base = caller
	}
	owner, err := d.OwnerForAgent(caller)
	return func(agentID string) (bool, error) { return d.ownerReaches(base, owner, err, agentID) }
}

// PrincipalReaches is the access rule for a caller holding the credential
// p on any entry point that opens bytes to an agent (HTTP /connect and
// data sessions, as the gRPC paths through agentReaches): the same agent,
// an agent under p's subject, or an agent of p's owner (OwnerForAgent:
// the principal it connected with here, else its directory record, so an
// agent on another replica or of an outside provider resolves too). The
// relay's in-process agents are never reached this way. An owner the
// directory could not read is an error (ownerReaches), not "no".
func (d *Dispatcher) PrincipalReaches(p identity.Principal) func(agentID string) (bool, error) {
	base := p.Subject // the key base; a principal the relay made for an agent has only its agent ID
	if base == "" {
		base = p.AgentID
	}
	switch {
	case base == "":
		return func(string) (bool, error) { return false, nil }
	case d.SingleTenant:
		return func(agentID string) (bool, error) { return !d.IsInternalAgent(agentID), nil }
	}
	return func(agentID string) (bool, error) {
		if d.IsInternalAgent(agentID) {
			return false, nil
		}
		return d.ownerReaches(base, p.Owner, nil, agentID)
	}
}

// A2AReaches is agentReaches for agent-to-agent traffic, which also carries
// relay plumbing: an authenticated agent may reach the relay's internal
// agents, and an internal agent registered in this process may reach any
// agent.
func (d *Dispatcher) A2AReaches(caller, base, target string) (bool, error) {
	if caller == "" {
		return false, nil
	}
	if _, internal := d.internalAgents.Load(caller); internal || d.IsInternalAgent(target) {
		return true, nil
	}
	return d.agentReaches(caller, base, target)
}
