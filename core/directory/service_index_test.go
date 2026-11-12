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

// TestServiceAgentsIndex: the (account, group, service) index follows the
// records: a claim adds, a renewal keeps, a delete removes; a member whose
// record is gone is pruned when read, one whose record no longer
// publishes the service is skipped; another account's and an unpublished
// service's agents are never listed.
func TestServiceAgentsIndex(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	ctx := context.Background()
	mc := NewMemoryCache()
	inv, _ := NewIPInventory(testLogger(), mc, time.Second, "127.0.0.1", GetDispatcherCacheKey)
	store := inv.(SessionStore)
	idx := inv.(ServiceIndex)
	const pub = "pg=tcp+e2ee;group=db,web=http"
	claim := func(agent, account, services string) Session {
		s, err := store.ClaimSession(ctx, agent, Session{Since: time.Now(), Account: account, Services: services}, nil, nil, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	list := func() []string {
		got, err := idx.ServiceAgents(ctx, "alice", "db", "pg")
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(got)
		return got
	}
	a := claim("a", "alice", pub)
	b := claim("b", "alice", pub)
	claim("c", "bob", pub)                     // another account
	claim("d", "alice", "pg=tcp+e2ee")         // not published
	claim("e", "alice", "pg=tcp+e2ee;group=x") // another group
	if got := list(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("listed %v, want a and b", got)
	}
	if err := store.PutSession(ctx, "a", a, true, nil, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := inv.(interface {
		DeleteSession(context.Context, string, Session) error
	}).DeleteSession(ctx, "b", b); err != nil {
		t.Fatal(err)
	}
	if got := list(); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after b's delete: %v, want a", got)
	}
	// b again, then its record vanishes without a delete (its replica died).
	claim("b", "alice", pub)
	if _, err := mc.Del(ctx, GetDispatcherCacheKey("b")); err != nil {
		t.Fatal(err)
	}
	if got := list(); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("b's record gone: %v, want a", got)
	}
	if m, _ := mc.SInter(ctx, inv.(*ipInventory).serviceKey("alice", "db", "pg")); slices.Contains(m, "b") {
		t.Fatal("a member without a record was not pruned")
	}
	// a reconnects with pg no longer published: skipped.
	claim("a", "alice", "pg=tcp+e2ee")
	if got := list(); len(got) != 0 {
		t.Fatalf("a unpublished: %v, want none", got)
	}
}
