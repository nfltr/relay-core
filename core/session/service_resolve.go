// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// A service by name, "<service>@<group>" (nfltr
// docs/decisions/service-discovery-2026-11-12.md section 3): the relay
// picks one instance per open among the agents of the caller's account
// whose declarations publish the service under the group (the directory's
// ServiceIndex), and the client checks the instance's certificate. No
// state outlives the open (section 5: no TLA+ spec): the instances one
// open tried travel in its Target.Exclude.

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2" // nosemgrep: math-random-used -- spreads opens over instances, not a secret
	"slices"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// ResolveService picks the instance of a's service under a's group that
// an open of caller opens: among the agents of caller's account that
// publish it (online on any replica) and reaches allows, minus the ones
// exclude names (by agent ref, or by machine name: the machine names of
// the certificates the client refused), one whose service is up, else
// one whose service is down (its answer then says so: a stuck health flag
// never hides the last capacity); uniform random. No instance left is
// the answer an offline agent gets (AGENT_OFFLINE: a held open waits for
// one to connect).
func (d *Dispatcher) ResolveService(ctx context.Context, caller identity.Principal, reaches func(string) (bool, error), a AgentAddress, exclude []string) (string, *Refusal) {
	name := a.Service + "@" + a.Group
	refuse := func(reason relayv1.ErrorReason, msg string) (string, *Refusal) {
		return "", &Refusal{Reason: reason, Msg: msg, AgentID: name}
	}
	idx, ok := d.IPInventory.(directory.ServiceIndex)
	if !ok || caller.Owner == "" {
		return refuse(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, "no instance of "+name+" is online")
	}
	agents, err := idx.ServiceAgents(ctx, caller.Owner, a.Group, a.Service)
	if err != nil {
		return refuse(relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, "the relay's directory is unavailable")
	}
	excluded, err := d.excludedMachines(caller, exclude)
	if err != nil {
		return refuse(relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, "the relay's directory is unavailable")
	}
	var up, down []string
	var unavailable bool
	for _, agentID := range agents {
		ref, err := identity.ParseAgentRef(agentID)
		if err != nil || slices.Contains(exclude, agentID) || excluded[ref.Machine] {
			continue
		}
		switch ok, err := reaches(agentID); {
		case err != nil:
			unavailable = true
			continue
		case !ok:
			continue
		}
		if d.serviceDown(ctx, agentID, a.Service) {
			down = append(down, agentID)
		} else {
			up = append(up, agentID)
		}
	}
	pool := up
	if len(pool) == 0 {
		pool = down
	}
	switch {
	case len(pool) > 0:
		return pool[rand.IntN(len(pool))], nil // #nosec G404 -- load spreading, not a secret
	case unavailable:
		return refuse(relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, "the relay's directory is unavailable")
	}
	return refuse(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, "no instance of "+name+" is online")
}

// excludedMachines are the machines of caller's account that exclude names
// by machine name (the Policy's NameResolver; none without one).
func (d *Dispatcher) excludedMachines(caller identity.Principal, exclude []string) (map[identity.MachineID]bool, error) {
	r, ok := d.Policy.(NameResolver)
	if !ok || len(exclude) == 0 {
		return nil, nil
	}
	out := map[identity.MachineID]bool{}
	for _, name := range exclude {
		if !identity.ValidMachineName(name) {
			continue
		}
		m, err := r.ResolveMachine(caller, name)
		if err != nil {
			return nil, err
		}
		if m != "" {
			out[m] = true
		}
	}
	return out, nil
}

// serviceDown reports whether agentID marks service down (its label
// agentservices.HealthLabel is HealthDown, agentLabels).
func (d *Dispatcher) serviceDown(ctx context.Context, agentID, service string) bool {
	return d.agentLabels(ctx, agentID)[agentservices.HealthLabel(service)] == agentservices.HealthDown
}

// agentLabels are agentID's current labels, the one source of its
// services' health: its stream's here (UpdateLabels sets them), else its
// stream record's. A record that cannot be read has none (every service
// counts as up).
func (d *Dispatcher) agentLabels(ctx context.Context, agentID string) map[string]string {
	if s, ok := d.Upstreams.Load(agentID); ok {
		if l, ok := s.(interface{ Labels() map[string]string }); ok {
			return l.Labels()
		}
		return nil
	}
	if r, ok := d.StreamerIPInventory.(interface {
		AgentLabels(context.Context, string) (map[string]string, error)
	}); ok {
		labels, _ := r.AgentLabels(ctx, agentID)
		return labels
	}
	return nil
}

// openService is Open of a service by name: an instance that cannot be
// reached, or whose agent refuses the session (its target refused or
// unreachable), is tried no more this open, and another is picked before
// the client is answered; with none left the client gets the last
// failure once. The tried set lives only as long as the open.
func (c *Core) openService(ctx context.Context, caller identity.Principal, t Target) (Opened, error) {
	var last error
	tried := slices.Clone(t.Exclude)
	for {
		try := t
		try.Exclude = tried
		try.Hold = t.Hold && last == nil // an open held for an instance waits once, before any was tried
		out, err := c.openOnce(ctx, caller, try)
		agentID := out.AgentID
		if err == nil {
			if err = answered(ctx, out.Session); err == nil {
				return out, nil
			}
			_ = out.Close()
			if out.Release != nil {
				out.Release()
			}
		}
		var dial *DialError
		switch {
		case last != nil && errors.Is(err, ErrOffline) && !errors.As(err, &dial):
			return Opened{}, last // every instance was tried
		case errors.As(err, &dial):
			agentID = dial.AgentID
		}
		if !retryElsewhere(err) || ctx.Err() != nil {
			return Opened{}, err
		}
		last, tried = err, append(tried, agentID)
	}
}

// answered waits for s's answer when it gives one: nil once the agent took
// the session.
func answered(ctx context.Context, s peerlink.Session) error {
	if cs, ok := s.(peerlink.ChainSession); ok && cs.Answers() {
		return cs.Answered(ctx)
	}
	return nil
}

// retryElsewhere reports whether err, an instance's failure before the
// session opened, is that instance's alone: the agent could not be reached
// (offline, a replica draining or out of reach, the session lost before
// its answer) or answered that its target refused or could not be
// reached. Anything else (the caller's access, the session's mode, the
// relay's directory) is the same on every instance.
func retryElsewhere(err error) bool {
	var dial *DialError
	if errors.As(err, &dial) {
		return !errors.Is(err, ErrAgentAnswers) && !errors.Is(err, ErrDirectoryUnavailable)
	}
	var a *link.Abort
	if !errors.As(err, &a) {
		return false
	}
	reason, app := dataplane.AbortReason(a)
	switch {
	case !app:
		return a.Code != link.CodeTimeout // link's own: the path, not the caller
	case reason == relayv1.ErrorReason_ERROR_REASON_TARGET_REFUSED, reason == relayv1.ErrorReason_ERROR_REASON_TARGET_UNREACHABLE:
		return true
	}
	return false
}

// checkServiceTarget refuses a service by name whose open names another
// service in its target ("" or the name's own).
func checkServiceTarget(a AgentAddress, target string) *Refusal {
	if target == "" || target == a.Service {
		return nil
	}
	return &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT, AgentID: a.Service + "@" + a.Group,
		Msg: fmt.Sprintf("%s@%s names service %s: its target must be empty or %s", a.Service, a.Group, a.Service, a.Service)}
}
