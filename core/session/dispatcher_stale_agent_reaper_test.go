// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// storeTestAgent registers an external gRPC agent last seen at seen.
func storeTestAgent(t *testing.T, d *Dispatcher, agentID string, seen time.Time) {
	t.Helper()
	ticker := time.NewTicker(time.Hour)
	t.Cleanup(ticker.Stop)
	d.Upstreams.Store(agentID, NewGRPCStream(agentID, &fake.MockAgentTunnelServer{Ctx: context.Background()}, context.Background(), make(chan proto.Message, 1), ticker))
	d.agentSeenAt(agentID, seen)
}

// The running reaper evicts an agent idle past staleAgentMaxIdle and keeps
// one seen within it. The idle agent's disconnect is the event that a reap
// pass ran (no sleep: a loaded host may delay the ticker), and the active
// agent's idle bound is far beyond any scheduling delay.
func TestRunStaleAgentReaperEvictsIdleAgentKeepsActiveAgent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()

	d.staleAgentReaperInterval, d.staleAgentMaxIdle = 20*time.Millisecond, time.Minute

	evicted := make(chan string, 2)
	d.OnAgentDisconnect = func(agentID string) { evicted <- agentID }

	storeTestAgent(t, d, "idle-agent", time.Now().Add(-time.Hour))
	storeTestAgent(t, d, "active-agent", time.Now().Add(-time.Hour))
	d.touchAgentLastSeen("active-agent")

	go d.RunStaleAgentReaper(t.Context())

	select {
	case id := <-evicted:
		if id != "idle-agent" {
			t.Fatalf("evicted %q, want idle-agent", id)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the reaper never evicted the idle agent")
	}
	if _, ok := d.Upstreams.Load("idle-agent"); ok {
		t.Fatal("expected idle agent to be evicted from upstreams")
	}
	if _, ok := d.Upstreams.Load("active-agent"); !ok {
		t.Fatal("expected active agent to remain in upstreams")
	}
	select {
	case id := <-evicted:
		t.Fatalf("unexpected disconnect callback for %q", id)
	default:
	}
}

func TestStaleAgentReaperSkipsAgentWithoutLastSeenEvidence(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()

	d.staleAgentReaperInterval, d.staleAgentMaxIdle = 50*time.Millisecond, 20*time.Millisecond

	var disconnects atomic.Int32
	d.OnAgentDisconnect = func(agentID string) {
		if agentID == "unknown-agent" {
			disconnects.Add(1)
		}
	}

	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	grpcStream := NewGRPCStream("unknown-agent", &fake.MockAgentTunnelServer{Ctx: context.Background()}, context.Background(), heartbeats, ticker)
	d.Upstreams.Store("unknown-agent", grpcStream)

	d.reapStaleAgents()

	if _, ok := d.Upstreams.Load("unknown-agent"); !ok {
		t.Fatal("expected agent without last-seen evidence to remain in upstreams")
	}
	if disconnects.Load() != 0 {
		t.Fatalf("expected no disconnect callback, got %d", disconnects.Load())
	}
}

// The reaper of a relay with no agent holds no ticker; an agent that
// registers later (its wake) starts it and is still evicted when idle.
func TestRunStaleAgentReaperStartsOnFirstAgent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.staleAgentReaperInterval, d.staleAgentMaxIdle = 20*time.Millisecond, 10*time.Millisecond
	go d.RunStaleAgentReaper(t.Context())
	time.Sleep(50 * time.Millisecond)

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	d.Upstreams.Store("late-agent", NewGRPCStream("late-agent", &fake.MockAgentTunnelServer{Ctx: context.Background()}, context.Background(), make(chan proto.Message, 1), ticker))
	d.agentSeenAt("late-agent", time.Now().Add(-time.Minute))
	d.wakeStaleAgentReaper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := d.Upstreams.Load("late-agent"); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the reaper did not start for an agent that registered after it")
}

// An in-process agent is never reaped, however long it has been silent (it
// sends no heartbeats); an external one idle as long is.
func TestReapStaleAgentsSkipsInternalAgent(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.staleAgentReaperInterval, d.staleAgentMaxIdle = time.Hour, 20*time.Millisecond

	loop := NewInternalAgentLoopback(t.Context(), "internal-agent")
	deregister := d.RegisterInternalAgent(t.Context(), "internal-agent", loop.DispatcherSide())
	defer deregister()

	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	external := NewGRPCStream("external-agent", &fake.MockAgentTunnelServer{Ctx: context.Background()}, context.Background(), make(chan proto.Message, 1), ticker)
	d.Upstreams.Store("external-agent", external)

	old := time.Now().Add(-time.Minute)
	d.agentSeenAt("internal-agent", old)
	d.agentSeenAt("external-agent", old)
	d.reapStaleAgents()

	if _, ok := d.Upstreams.Load("internal-agent"); !ok {
		t.Fatal("internal agent was reaped")
	}
	if _, ok := d.Upstreams.Load("external-agent"); ok {
		t.Fatal("idle external agent was not reaped")
	}
}

// TestTouchAgentLastSeenAllocatesNothing is the guard of R3
// (docs/performance.md): recording a known agent's inbound frame costs no
// allocation, and the time read back is the time recorded.
func TestTouchAgentLastSeenAllocatesNothing(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.touchAgentLastSeen("a")
	if n := testing.AllocsPerRun(1000, func() { d.touchAgentLastSeen("a") }); n != 0 {
		t.Fatalf("%v allocations per inbound frame, want 0", n)
	}
	at := time.Now().Add(-time.Minute)
	d.agentSeenAt("a", at)
	if got, ok := d.AgentLastSeen("a"); !ok || !got.Equal(at) || time.Since(got) < time.Minute {
		t.Fatalf("last seen %v %v, want %v", got, ok, at)
	}
}
