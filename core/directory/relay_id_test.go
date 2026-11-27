// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"sync"
	"testing"

	"github.com/nfltr/relay/proto/wire/agentwire"
)

// One relay id per directory: every replica sharing one store reads the
// same id, even when they start at once; another directory has its own
// (spec/Failover.tla, Welcome.relay_id).
func TestRelayIDOnePerDirectory(t *testing.T) {
	ctx := context.Background()
	shared := NewMemoryCache()
	ids := make([]string, 8)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() {
			id, err := RelayID(ctx, shared)
			if err != nil {
				t.Error(err)
			}
			ids[i] = id
		})
	}
	wg.Wait()
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("replicas of one directory read %q", ids)
		}
	}
	// The id is a Welcome's: the agent reads it back as sent.
	w := agentwire.WelcomeOf("v", ids[0])
	if ids[0] == "" || agentwire.WelcomeRelayID(map[string][]string{w[0]: {w[1]}}) != ids[0] {
		t.Fatalf("id %q does not survive a Welcome", ids[0])
	}
	other, err := RelayID(ctx, NewMemoryCache())
	if err != nil || other == ids[0] {
		t.Fatalf("an independent directory's id %q (%v), the first's %q", other, err, ids[0])
	}
}

// A store that cannot write a key only when it is absent gets no id (fail
// closed: two replicas could otherwise each write their own).
func TestRelayIDNeedsSetNX(t *testing.T) {
	if id, err := RelayID(context.Background(), struct{ Cache }{NewMemoryCache()}); err == nil {
		t.Fatalf("id %q from a store without SetNX", id)
	}
}

func TestMemoryCacheSetNX(t *testing.T) {
	ctx := context.Background()
	m := NewMemoryCache()
	if ok, err := m.SetNX(ctx, "k", "a", 0); !ok || err != nil {
		t.Fatalf("first SetNX = %v, %v", ok, err)
	}
	if ok, err := m.SetNX(ctx, "k", "b", 0); ok || err != nil {
		t.Fatalf("second SetNX = %v, %v", ok, err)
	}
	if v, _ := m.Get(ctx, "k"); v == nil || v.PodIP != "a" {
		t.Fatalf("Get = %+v, want the first value", v)
	}
}
