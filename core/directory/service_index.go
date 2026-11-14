// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"errors"

	"github.com/nfltr/relay/proto/wire/agentservices"
)

// ServiceIndex is implemented by the shared inventory: the agents of an
// account whose records declare a service published under an organisation
// group (agentservices.Decl.Group, dialled as <service>@<group>), kept by
// the records' writes (PutSession: claims and renewals) and deletes
// (DeleteSession). The index is a set beside the records, never their
// source: a member whose record is gone (its replica died, the record
// expired) is pruned when read, and one whose record no longer publishes
// the service is skipped.
type ServiceIndex interface {
	// ServiceAgents returns the agents of account whose live records
	// publish service under group, in no particular order.
	ServiceAgents(ctx context.Context, account, group, service string) ([]string, error)
}

var _ ServiceIndex = (*ipInventory)(nil)

// serviceKey is the index set of (account, group, service): group and
// service are DNS labels, so the account (which may hold any byte) comes
// last.
func (d *ipInventory) serviceKey(account, group, service string) string {
	return "inventory:svc:" + d.getCacheKey("") + group + "/" + service + "/" + account
}

// serviceKeys are the index sets session s's declaration belongs to (none
// without an account or a published service).
func (d *ipInventory) serviceKeys(s Session) []string {
	if s.Account == "" || s.Services == "" {
		return nil
	}
	decls, err := agentservices.Parse(s.Services)
	if err != nil { // written from a declaration the relay accepted: never
		return nil
	}
	var keys []string
	for _, decl := range decls {
		if decl.Group != "" {
			keys = append(keys, d.serviceKey(s.Account, decl.Group, decl.Name))
		}
	}
	return keys
}

// sessionSets are the sets an agent's record for session s puts it in:
// the active set and the index sets of its declaration.
func (d *ipInventory) sessionSets(s Session) []string {
	return append(append([]string{d.activeAgentSetKey()}, d.serviceKeys(s)...), d.nf1Keys(s)...)
}

// recordGone matches no record.
func recordGone(v *Value) bool { return v == nil }

// prune removes from set the members whose record matches gone (and the
// record), each in one store step with that check: a claim landing
// meanwhile keeps its entry (spec/README.md finding 47).
func (d *ipInventory) prune(ctx context.Context, set string, members []string, gone func(*Value) bool) error {
	for _, m := range members {
		if _, err := d.inventory.DelIf(ctx, d.getCacheKey(m), gone, m, set); err != nil {
			return err
		}
	}
	return nil
}

// ServiceAgents implements ServiceIndex.
func (d *ipInventory) ServiceAgents(ctx context.Context, account, group, service string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	key := d.serviceKey(account, group, service)
	members, err := d.inventory.SInter(ctx, key)
	if err != nil {
		return nil, err
	}
	var out, stale []string
	for _, agentID := range members {
		v, err := d.inventory.Get(ctx, d.getCacheKey(agentID))
		switch {
		case errors.Is(err, ErrNotFound) || err == nil && v == nil:
			stale = append(stale, agentID)
			continue
		case err != nil:
			return nil, err
		}
		if publishes(v.Services, group, service) {
			out = append(out, agentID)
		}
	}
	if err := d.prune(ctx, key, stale, recordGone); err != nil {
		d.logger.Warn("failed to prune stale service index members", "count", len(stale), "error", err)
	}
	return out, nil
}

// publishes reports whether declaration services publishes service under
// group.
func publishes(services, group, service string) bool {
	decls, err := agentservices.Parse(services)
	if err != nil {
		return false
	}
	decl, ok := agentservices.Find(decls, service)
	return ok && decl.Group == group
}

// AgentLabels returns the labels agentID's record carries (nil: no
// record).
func (d *ipInventory) AgentLabels(ctx context.Context, agentID string) (map[string]string, error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	v, err := d.inventory.Get(ctx, d.getCacheKey(agentID))
	if err != nil || v == nil {
		if errors.Is(err, ErrNotFound) {
			err = nil
		}
		return nil, err
	}
	return v.Labels, nil
}
