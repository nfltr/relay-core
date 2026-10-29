// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"uuid"

	"github.com/nfltr/relay/client/rpcclient"
	"google.golang.org/grpc/metadata"
)

// The stale-agent reaper evicting an agent's AgentTunnel leaves the state
// its live ConnectHTTP session still uses.
func TestReaperEvictionKeepsSiblingSessionState(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	const id = "acct.site"
	loop := NewInternalAgentLoopback(context.Background(), id)
	defer loop.Close()
	d.Agents.Store(id, &HTTPStream{ID: id})
	d.RegisterAgent(id, "acct", "", "")
	d.Upstreams.Store(id, loop.DispatcherSide())
	d.RegisterAgent(id, "acct", "", "")

	d.disconnectAgent(id)
	if _, ok := d.Upstreams.Load(id); ok {
		t.Fatal("the reaper left the evicted stream")
	}
	if base, _ := d.AgentKeyBase.Load(id); base != "acct" {
		t.Fatalf("the reaper removed the key base the agent's HTTP session uses (%q)", base)
	}
}

// An internal agent's cleanup removes only its own registration, never a
// newer stream registered under its id.
func TestInternalAgentCleanupLeavesNewerStream(t *testing.T) {
	d := newTestDispatcherForInternalAgent(t)
	loop := NewInternalAgentLoopback(context.Background(), "test-relay-agent")
	defer loop.Close()
	cleanup := d.RegisterInternalAgent(context.Background(), loop.AgentID(), loop.DispatcherSide())
	newer := &GRPCStream{}
	d.Upstreams.Store(loop.AgentID(), newer)
	cleanup()
	if cur, _ := d.Upstreams.Load(loop.AgentID()); cur != Streamer(newer) {
		t.Fatal("the internal agent's cleanup removed the newer stream")
	}
}

// Concurrent connects with one agent id: whatever their interleaving, one
// session stays open and it is the routed one (upstreams). Store and Swap
// done apart let a connect that stored first swap last: it cancelled the
// routed session and stayed open unrouted (CI 2026-10-15,
// TestAgentTunnelTakeoverKeepsAgentState's first session stayed open).
func TestAgentTunnelConcurrentConnectsRouteTheSurvivor(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const id, n = "acct.node", 8
	agents := make([]*rpcclient.Agent, n)
	var wg sync.WaitGroup
	for i := range agents {
		wg.Go(func() {
			md := metadata.Pairs("x-apikey-agentID", id, "x-apikey-base-id", "acct", "x-auth-method", "apikey",
				rpcclient.InstanceHeader, uuid.New().String())
			tunnel, err := client.AgentTunnel(metadata.NewOutgoingContext(ctx, md))
			if err != nil {
				t.Error(err)
				return
			}
			agents[i] = rpcclient.NewAgent(id, tunnel)
		})
	}
	wg.Wait()
	for _, a := range agents {
		if a != nil {
			defer a.Close() //nolint:errcheck
		}
	}
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		open := 0
		for _, a := range agents {
			select {
			case <-a.Done():
			default:
				open++
			}
		}
		cur, ok := d.Upstreams.Load(id)
		if open == 1 && ok && cur.(*GRPCStream).streamCtx.Err() == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions open, routed session live: %v", open, ok && cur.(*GRPCStream).streamCtx.Err() == nil)
		}
	}
}
