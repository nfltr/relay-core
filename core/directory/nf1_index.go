// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/nfltr/relay/proto/wire/agentservices"
)

// NF1Index is implemented by the shared inventory: the agents whose live
// records declare a service under a capability address's rendezvous id
// (agentservices.Decl.NF1), whatever their account (nfltr
// docs/decisions/nf1-rendezvous-2026-11-13.md). Kept like ServiceIndex: a
// set beside the records, written by PutSession, removed by DeleteSession,
// pruned and checked against the records when read, never their source.
type NF1Index interface {
	// NF1Agents returns the agents whose live records declare rid.
	NF1Agents(ctx context.Context, rid [agentservices.RIDSize]byte) ([]string, error)
}

var _ NF1Index = (*ipInventory)(nil)

func (d *ipInventory) nf1Key(rid [agentservices.RIDSize]byte) string {
	return "inventory:nf1:" + d.getCacheKey("") + hex.EncodeToString(rid[:])
}

// nf1Keys are the index sets session s's declaration belongs to.
func (d *ipInventory) nf1Keys(s Session) []string {
	if s.Services == "" {
		return nil
	}
	decls, err := agentservices.Parse(s.Services)
	if err != nil { // written from a declaration the relay accepted: never
		return nil
	}
	var keys []string
	for _, decl := range decls {
		if decl.NF1 != ([agentservices.RIDSize]byte{}) {
			keys = append(keys, d.nf1Key(decl.NF1))
		}
	}
	return keys
}

// NF1Agents implements NF1Index.
func (d *ipInventory) NF1Agents(ctx context.Context, rid [agentservices.RIDSize]byte) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, d.opTimeout)
	defer cancel()
	key := d.nf1Key(rid)
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
		if decls, err := agentservices.Parse(v.Services); err == nil {
			if _, ok := agentservices.FindNF1(decls, rid); ok {
				out = append(out, agentID)
			}
		}
	}
	if err := d.prune(ctx, key, stale); err != nil {
		d.logger.Warn("failed to prune stale nf1 index members", "count", len(stale), "error", err)
	}
	return out, nil
}
