// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
)

func recordOf(mc *fake.MockCache, agentID string) *directory.Value {
	mc.Mu.Lock()
	defer mc.Mu.Unlock()
	return mc.Data[directory.GetDispatcherCacheKey(agentID)]
}

func dropRecord(mc *fake.MockCache, agentID string) {
	mc.Mu.Lock()
	defer mc.Mu.Unlock()
	delete(mc.Data, directory.GetDispatcherCacheKey(agentID))
}

// TestRebalanceMoveKeepsRenewingTheRecord (spec/README.md finding 49,
// Move.tla RecordLives): a rebalancing move whose agent's tries all land
// back here (no new session takes the record) keeps renewing the old
// session's record, so it never expires while this replica routes the
// agent. The code before stopped renewing at the ask: an 8 s move expired
// the record and every peer's new session got 503 (nfltr's replica-link
// fault rig, case redis).
func TestRebalanceMoveKeepsRenewingTheRecord(t *testing.T) {
	d, s, sink, mc, inv, _ := movingSession(t)
	d.moveBound = time.Minute
	d.Config.HeartbeatRecvThreshold = 200 * time.Millisecond // TTL 200 ms, a renewal every 100 ms
	s.rebalance = make(chan struct{})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.WatchAgentSession(ctx, "agent-1", s, inv, make(chan error)) }()
	defer func() { cancel(); <-done }()
	if got := d.rebalanceAgents(0, 2); got != 1 {
		t.Fatalf("asked %d, want 1", got)
	}
	waitMove(t, "MOVE_RECONNECT", func() bool { return len(sink.steps()) == 1 })
	dropRecord(mc, "agent-1") // its TTL passed since the last renewal
	waitMove(t, "the record renewed while the move is asked", func() bool { return recordOf(mc, "agent-1") != nil })
	if !s.moving.Load() || !agentConnected(d, "agent-1") {
		t.Fatal("the move ended: the test checked nothing")
	}
}

// TestTickRenewalNotThrottledByReannounce (finding 49): the renewal
// ticker's write is not skipped because the re-announce wrote the record
// just before, off the ticker's phase (the throttle is the inbound frames'
// own): skipped, the re-announced record went unrenewed for up to its whole
// TTL. Frames stay throttled.
func TestTickRenewalNotThrottledByReannounce(t *testing.T) {
	d, s, _, mc, inv, _ := movingSession(t)
	d.Config.HeartbeatRecvThreshold = time.Hour
	if err := d.rewriteRecord(t.Context(), "agent-1", inv, s); err != nil {
		t.Fatal(err)
	}
	dropRecord(mc, "agent-1")
	if err := d.renewAgentInventory(t.Context(), "agent-1", inv, s); err != nil {
		t.Fatal(err)
	}
	if recordOf(mc, "agent-1") == nil {
		t.Fatal("the ticker's renewal was throttled by the re-announce's write")
	}
	dropRecord(mc, "agent-1")
	if err := d.refreshAgentInventoryOnInboundActivity(t.Context(), "agent-1", inv, s); err != nil {
		t.Fatal(err)
	}
	if recordOf(mc, "agent-1") != nil {
		t.Fatal("an inbound frame renewed within the throttle interval")
	}
}
