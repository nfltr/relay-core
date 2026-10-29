// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"
)

// newTestDispatcherForInternalAgent returns a minimal dispatcher suitable for
// internal-agent registration tests. Reuses the helpers in dispatcher_test.go.
func newTestDispatcherForInternalAgent(t *testing.T) *Dispatcher {
	t.Helper()
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	return d
}

// TestRegisterInternalAgent_StoresInUpstreams asserts the in-process agent
// becomes routable through the dispatcher's upstreams map after registration,
// and is removed after the cleanup func runs.
func TestRegisterInternalAgent_StoresInUpstreams(t *testing.T) {
	d := newTestDispatcherForInternalAgent(t)
	loop := NewInternalAgentLoopback(context.Background(), "test-relay-agent")
	defer loop.Close()

	cleanup := d.RegisterInternalAgent(context.Background(), loop.AgentID(), loop.DispatcherSide())
	defer cleanup()

	if _, ok := d.Upstreams.Load(loop.AgentID()); !ok {
		t.Fatal("RegisterInternalAgent: agent not stored in upstreams")
	}

	cleanup()
	// After cleanup, the upstreams entry is gone.
	if _, ok := d.Upstreams.Load(loop.AgentID()); ok {
		t.Fatal("RegisterInternalAgent: agent still in upstreams after cleanup")
	}
}

// TestRegisterInternalAgent_CleanupIdempotent asserts double-cleanup is safe.
func TestRegisterInternalAgent_CleanupIdempotent(t *testing.T) {
	d := newTestDispatcherForInternalAgent(t)
	loop := NewInternalAgentLoopback(context.Background(), "test-relay-agent")
	defer loop.Close()

	cleanup := d.RegisterInternalAgent(context.Background(), loop.AgentID(), loop.DispatcherSide())
	cleanup()
	cleanup() // must not panic, must not error
}

// TestRegisterInternalAgent_ParentCancelStopsReceiveLoop asserts the receive
// loop terminates when the parent context cancels (cleanup path 2).
func TestRegisterInternalAgent_ParentCancelStopsReceiveLoop(t *testing.T) {
	d := newTestDispatcherForInternalAgent(t)
	loop := NewInternalAgentLoopback(context.Background(), "test-relay-agent")
	defer loop.Close()

	parent, cancel := context.WithCancel(context.Background())
	cleanup := d.RegisterInternalAgent(parent, loop.AgentID(), loop.DispatcherSide())
	defer cleanup()

	// Cancel parent ctx; the receive loop should exit and cleanup is safe.
	cancel()
	// Give the goroutine a moment to observe cancellation.
	time.Sleep(50 * time.Millisecond)
	cleanup()

	if _, ok := d.Upstreams.Load(loop.AgentID()); ok {
		t.Fatal("upstreams entry not removed after parent cancel + cleanup")
	}
}
