// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
)

// downCache is a directory whose reads fail (Redis unreachable).
type downCache struct{ directory.Cache }

func (downCache) Get(context.Context, string) (*directory.Value, error) {
	return nil, errors.New("dial tcp: lookup rpc-redis-master: i/o timeout")
}

// TestOwnerForAgentReportsDirectoryErrors: a directory that cannot be read
// is an error from OwnerForAgent and from the reach rules, not "" and
// false, which their callers answered as the agent being offline or not
// found (the 24 h soak's partition heal: fast 503 "offline" from a replica
// whose Redis dials failed). A directory that has no record is no error.
func TestOwnerForAgentReportsDirectoryErrors(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.SingleTenant = false
	use := func(c directory.Cache) {
		d.IPInventory, _ = directory.NewIPInventory(fake.TestLogger(), c, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		d.StreamerIPInventory, _ = directory.NewIPInventory(fake.TestLogger(), c, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	}
	use(downCache{directory.NewMemoryCache()})
	carol, bobHub := testRef("carol", "node"), testRef("bob", "hub")
	d.Policy = AccountIsOneGroup{}
	d.RegisterAgent(bobHub, "keys:bob", "")
	if owner, err := d.OwnerForAgent(carol); owner != "" || !errors.Is(err, directory.ErrUnavailable) {
		t.Fatalf("OwnerForAgent with the directory down = %q, %v; want ErrUnavailable", owner, err)
	}
	bob := identity.Principal{Provider: "keys", Subject: "bob", Owner: "keys:bob", Machine: testMachine("bob")}
	if ok, err := d.Reach(bob, carol, Dial); ok || !errors.Is(err, directory.ErrUnavailable) {
		t.Fatalf("Reach(another account's agent) with the directory down = %v, %v; want false, ErrUnavailable", ok, err)
	}
	if ok, err := d.A2AReaches(bobHub, carol); ok || !errors.Is(err, directory.ErrUnavailable) {
		t.Fatalf("A2AReaches with the directory down = %v, %v; want false, ErrUnavailable", ok, err)
	}

	use(directory.NewMemoryCache())
	if owner, err := d.OwnerForAgent(carol); owner != "" || err != nil {
		t.Fatalf("OwnerForAgent of an agent with no record = %q, %v; want \"\", nil", owner, err)
	}
	if ok, err := d.Reach(bob, carol, Dial); ok || err != nil {
		t.Fatalf("Reach(an agent with no record) = %v, %v; want false, nil", ok, err)
	}
}
