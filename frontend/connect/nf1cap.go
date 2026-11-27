// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay-core/core/nf1cap"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// errRelayCap is a capped leg's error past its cut.
var errRelayCap = &link.Abort{Code: dataplane.AbortCode(nf1cap.Reason), Msg: nf1cap.Msg}

// capped is out, the onward leg of an nf1 session, counted as leg against
// its address's daily cap (nfltr docs/decisions/nf1-relay-cap-2026-11-25.md,
// spec/NF1Cap.tla Relay): what fits today is carried, then both legs are
// aborted with ERROR_REASON_LIMIT_EXCEEDED.
func capped(in, out peerlink.Session, leg *nf1cap.Leg) peerlink.Session {
	b := &budget{in: in, out: out, count: leg.Take, end: leg.End, cutErr: errRelayCap}
	return b.wrap()
}

// countsAgainstCap reports whether caller's session to agentID counts
// against the address's cap: every client but the agent's owner's own
// account (an owner not known counts: fail closed); never an anonymous
// agent's, which nf1anon budgets (spec/NF1Cap.tla Counted, NoCapOnOwner).
func (f *Frontend) countsAgainstCap(caller identity.Principal, agentID string) bool {
	if nf1anon.IsAgent(agentID) {
		return false
	}
	if caller.Owner == "" {
		return true
	}
	owner, err := f.Core.Dispatcher.OwnerForAgent(agentID)
	return err != nil || owner == "" || owner != caller.Owner
}

// admitCap is the counted leg of an open of agentID: the cap its label of
// the service declaring rid sets (Learn), and a re-read of it for the open
// legs; a refusal when the address is at its cap today (spec/NF1Cap.tla
// Open).
func (f *Frontend) admitCap(caps *nf1cap.Registry, def int64, rid [agentservices.RIDSize]byte, agentID string) (*nf1cap.Leg, error) {
	capOf := func() int64 { // bounded by the inventory's op timeout
		services, _ := f.Core.Dispatcher.AgentServices(agentID)
		d, ok := agentservices.FindNF1(services, rid)
		if !ok {
			return def
		}
		return nf1cap.CapOf(f.Core.Dispatcher.AgentLabels(context.Background(), agentID), nf1cap.Label(d.Target()), def)
	}
	leg := caps.Admit(rid, capOf(), capOf)
	if leg == nil {
		metrics.RecordConnectSession(true, nf1cap.Reason.String())
		return nil, dataplane.Refusal(nf1cap.Reason, nf1cap.Msg)
	}
	return leg, nil
}
