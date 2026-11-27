// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/spf13/viper"
)

// TestNF1AgentsIndex: the rendezvous index (nfltr
// docs/decisions/nf1-rendezvous-2026-11-13.md section 1) follows the
// records whatever their account: a claim adds, a delete removes, a member
// whose record is gone is pruned when read, one whose record no longer
// declares the rid is skipped, and a store that lost the index gets it back
// at the next renewal. Two inventories over one store (two replicas) see
// each other's entries.
func TestNF1AgentsIndex(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	ctx := context.Background()
	mc := NewMemoryCache()
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	other, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	store := inv.(SessionStore)
	rid := [16]byte{0xab, 1}
	const decl = "clip=tcp+e2ee;nf1=ab010000000000000000000000000000"
	claim := func(agent, account, services string) Session {
		s, err := store.ClaimSession(ctx, agent, Session{Since: time.Now(), Account: account, Services: services}, nil, nil, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	list := func(inv IPInventory) []string {
		got, err := inv.(NF1Index).NF1Agents(ctx, rid)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		return got
	}
	a := claim("a", "", decl) // no account: still indexed
	claim("b", "bob", "clip=tcp+e2ee;nf1=cd010000000000000000000000000000")
	if got := list(other); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("listed %v, want a", got)
	}
	if err := inv.(interface {
		DeleteSession(context.Context, string, Session) error
	}).DeleteSession(ctx, "a", a); err != nil {
		t.Fatal(err)
	}
	if got := list(inv); len(got) != 0 {
		t.Fatalf("after a's delete: %v", got)
	}
	a = claim("a", "", decl)
	key := inv.(*ipInventory).nf1Key(rid)
	if _, err := mc.Del(ctx, key); err != nil { // the store lost the index
		t.Fatal(err)
	}
	if err := store.PutSession(ctx, "a", a, true, nil, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := list(inv); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after a renewal: %v, want a", got)
	}
	claim("c", "carol", decl) // a second agent declaring the rid: both listed, the caller decides
	if got := list(inv); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("listed %v, want a and c", got)
	}
	if _, err := mc.Del(ctx, GetDispatcherCacheKey("c")); err != nil {
		t.Fatal(err)
	}
	claim("a", "", "clip=tcp+e2ee") // a no longer declares the rid
	if got := list(inv); len(got) != 0 {
		t.Fatalf("stale members listed: %v", got)
	}
	if m, _ := mc.SInter(ctx, key); slices.Contains(m, "c") {
		t.Fatal("a member without a record was not pruned")
	}
}
