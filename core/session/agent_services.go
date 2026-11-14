// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"slices"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/client/capaddr"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// declaredServices reads the services a ConnectHTTP stream declares: nil
// for none, an InvalidArgument status for a malformed declaration or a
// capability address's declaration without a registration proof that
// holds at one of hosts (nf1.hosts) (the session is refused, nothing is
// declared). The proofs are checked and dropped: never part of a Decl, the
// record, the echo or a log (nfltr docs/decisions/nf1-risks-2026-11-14.md,
// spec/NF1Claim.tla Accepts).
func declaredServices(ctx context.Context, hosts []string) ([]agentservices.Decl, error) {
	vals := metadata.ValueFromIncomingContext(ctx, agentservices.Header)
	switch len(vals) {
	case 0:
		return nil, nil
	case 1:
	default:
		return nil, status.Errorf(codes.InvalidArgument, "%s: declare the services once", agentservices.Header)
	}
	decls, err := agentservices.Parse(vals[0])
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%s: %v", agentservices.Header, err)
	}
	if err := checkRegistrations(ctx, decls, hosts); err != nil {
		return nil, err
	}
	return decls, nil
}

// checkRegistrations refuses decls unless each capability address's rid
// comes with a registration proof from its server key for one of hosts:
// a hard cut, an address minted before rids were bound to keys is refused
// too (owner decision 2026-11-14).
func checkRegistrations(ctx context.Context, decls []agentservices.Decl, hosts []string) error {
	var regs map[[agentservices.RIDSize]byte][]byte
	for _, d := range decls {
		if d.NF1 == ([agentservices.RIDSize]byte{}) {
			continue
		}
		if len(hosts) == 0 {
			return status.Errorf(codes.InvalidArgument, "service %s: this relay serves no capability addresses (nf1.hosts)", d.Name)
		}
		if regs == nil {
			var err error
			if regs, err = agentservices.ParseReg(metadata.ValueFromIncomingContext(ctx, agentservices.RegHeader)); err != nil {
				// ParseReg's message may quote a value: not echoed, as a proof is not.
				return status.Errorf(codes.InvalidArgument, "%s: malformed", agentservices.RegHeader)
			}
		}
		proof, ok := regs[d.NF1]
		if !ok {
			return status.Errorf(codes.InvalidArgument, "service %s: its capability address comes with no registration proof (%s): update nfltr, and mint the address again (--new-address)", d.Name, agentservices.RegHeader)
		}
		if !slices.ContainsFunc(hosts, func(h string) bool { return capaddr.CheckRegistration(h, d.NF1, proof) == nil }) {
			return status.Errorf(codes.InvalidArgument, "service %s: its capability address's registration proof does not hold at this relay (%s): an address minted before this relay's release, or for another relay, needs --new-address", d.Name, agentservices.RegHeader)
		}
	}
	return nil
}

// AgentServices returns the services agentID's ConnectHTTP stream
// declared, and whether the agent is connected: here, or on another
// replica, whose agent record in the shared inventory carries the
// declaration (directory.Session.Services). A record without one declares
// none (also one a relay before v0.20.3 wrote).
func (d *Dispatcher) AgentServices(agentID string) ([]agentservices.Decl, bool) {
	if s, ok := d.Agents.Load(agentID); ok {
		return s.Services, true
	}
	store, ok := d.IPInventory.(directory.SessionStore)
	if !ok || agentID == "" {
		return nil, false
	}
	_, s, err := store.GetSession(context.Background(), agentID) // bounded by the inventory's op timeout
	if err != nil {
		return nil, false
	}
	if s.Services == "" {
		return nil, true
	}
	services, err := agentservices.Parse(s.Services)
	if err != nil { // written by a relay from a declaration it accepted: never
		return nil, true
	}
	return services, true
}
