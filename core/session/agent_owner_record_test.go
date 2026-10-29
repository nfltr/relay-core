// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/core/directory"
)

// An agent's owner reaches every replica through its directory record: the
// agent connects to replica B (its principal names owner "keys:bob.node"),
// replica A, which never saw its connection, reads the owner from the
// record, and reach follows it. A record an older replica wrote names no
// owner: the agent is reached only from its caller's own namespace, as an
// unowned agent was before.
func TestAgentOwnerReachesAnotherReplicaThroughItsRecord(t *testing.T) {
	viper.Set("pod_ip.env_var", "TEST_POD_IP") // records name a pod
	t.Cleanup(func() { viper.Set("pod_ip.env_var", "") })
	t.Setenv("TEST_POD_IP", "10.0.0.2")
	mc := directory.NewMemoryCache()
	shared := func(d *Dispatcher) {
		d.IPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
		d.StreamerIPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	}
	a, cancelA := newTestDispatcher(t)
	defer cancelA()
	b, cancelB := newTestDispatcher(t)
	defer cancelB()
	shared(a)
	shared(b)
	a.SingleTenant, b.SingleTenant = false, false

	const agent, owner = "bob.node", "keys:bob.node"
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, b, agent))
	if _, err := client.AgentTunnel(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ConnectHTTP(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for mustOwner(t, b, agent) != owner {
		if time.Now().After(deadline) {
			t.Fatalf("replica B: owner of its agent = %q, want %q", mustOwner(t, b, agent), owner)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Both of its records (AgentTunnel's, ConnectHTTP's) name the owner, and
	// so do their renewals (they write the stream's Session).
	for name, inv := range map[string]directory.IPInventory{"tunnel": a.StreamerIPInventory, "http": a.IPInventory} {
		for {
			if _, s, err := inv.(directory.SessionStore).GetSession(t.Context(), agent); err == nil && s.Account == owner {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the agent's %s record names no owner", name)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if s, _ := b.Upstreams.Load(agent); s == nil || s.(*GRPCStream).Session().Account != owner {
		t.Fatal("the tunnel session's renewals would drop its owner")
	}
	if _, local := a.AgentOwners.Load(agent); local {
		t.Fatal("replica A must not know the agent from a connection")
	}
	if got := mustOwner(t, a, agent); got != owner {
		t.Fatalf("replica A: owner from the record = %q, want %q", got, owner)
	}
	a.RegisterAgent("bob.hub", "bob", owner, "")          // the same owner, on A
	a.RegisterAgent("eve.hub", "eve", "keys:eve.hub", "") // another owner, on A
	if !mustReach(t, a.A2AReaches, "bob.hub", "bob", agent) {
		t.Error("the owner's agent on replica A must reach its agent on replica B")
	}
	if mustReach(t, a.A2AReaches, "eve.hub", "eve", agent) {
		t.Error("another owner's agent must not reach it")
	}

	// A record as an older relay writes it: no account.
	if _, err := mc.Set(t.Context(), directory.GetStreamerCacheKey("old.node"), directory.Value{PodIP: "10.0.0.3", Since: time.Now().UnixNano(), Seq: 1, Credential: "tag"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := mustOwner(t, a, "old.node"); got != "" {
		t.Fatalf("owner of an older replica's record = %q, want none", got)
	}
	a.RegisterAgent("old.hub", "old", owner, "")
	if !mustReach(t, a.A2AReaches, "old.hub", "old", "old.node") {
		t.Error("an agent whose owner is unknown must still be reached from its own namespace")
	}
	if mustReach(t, a.A2AReaches, "bob.hub", "bob", "old.node") {
		t.Error("an agent whose owner is unknown must not be reached from another namespace")
	}
}

// mustOwner is d.OwnerForAgent for a test whose directory answers.
func mustOwner(t *testing.T, d *Dispatcher, agentID string) string {
	t.Helper()
	owner, err := d.OwnerForAgent(agentID)
	if err != nil {
		t.Fatalf("OwnerForAgent(%q): %v", agentID, err)
	}
	return owner
}

// mustReach is a reach rule's answer for a test whose directory answers.
func mustReach(t *testing.T, reaches func(caller, base, target string) (bool, error), caller, base, target string) bool {
	t.Helper()
	ok, err := reaches(caller, base, target)
	if err != nil {
		t.Fatalf("reach(%q, %q): %v", caller, target, err)
	}
	return ok
}
