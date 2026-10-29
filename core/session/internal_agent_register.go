// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package relay — internal_agent_register.go
//
// Phase 2 of option (A) refactor: a public Dispatcher method that registers
// an in-process agent (one half of an InternalAgentLoopback) as if it had
// connected over gRPC. Used by cmd/server to make the relay-internal
// orchestrator agent visible to the dispatcher's routing layer so A2A
// calls between connected workers and the relay-owned orchestrator work
// like any other agent-to-agent call.
package session

import (
	"context"
	"sync"
	"time"

	"github.com/nfltr/relay/client/rpcclient"
	"google.golang.org/protobuf/proto"
)

type contextBindableStreamer interface {
	WithContext(context.Context) Streamer
}

// RegisterInternalAgent registers an in-process agent with the dispatcher's
// routing layer. The returned cleanup func deregisters the agent and stops
// the receive loop; it is safe to call multiple times.
//
// The streamer is typically the DispatcherSide of an InternalAgentLoopback.
// The agentID is the stable identity (e.g. "nfltr-relay") the
// A2A routing layer matches against.
//
// Heartbeats and recv-threshold tickers are no-ops for in-process agents —
// there is no network to detect, and the parent ctx alone gates lifetime.
func (d *Dispatcher) RegisterInternalAgent(parent context.Context, agentID string, streamer Streamer) (cleanup func()) {
	ctx, cancel := context.WithCancel(parent)
	if bindable, ok := streamer.(contextBindableStreamer); ok {
		streamer = bindable.WithContext(ctx)
	}
	// Long-period ticker so the heartbeat watchdog never fires for the
	// in-process stream; the parent ctx governs lifetime instead.
	heartbeats := make(chan proto.Message, 1)
	ticker := time.NewTicker(time.Hour)
	grpcStream := NewGRPCStream(agentID, streamer, ctx, heartbeats, ticker)

	d.internalAgents.Store(agentID, struct{}{})
	d.Upstreams.Store(agentID, grpcStream)
	d.notifyUpstreamRegistered(agentID)
	d.touchAgentLastSeen(agentID)
	d.RegisterAgent(agentID, "", "", "internal-loopback")

	var receiveDone sync.WaitGroup
	receiveDone.Go(func() {
		// receiveFromGRPCAgent runs until the stream's context is cancelled
		// (e.g. via the returned cleanup func) or the streamer returns an
		// unrecoverable Recv error. It owns the inbound side of the agent's
		// duplex from the dispatcher's perspective.
		_ = d.receiveFromGRPCAgent(ctx, grpcStream)
	})

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			ticker.Stop()
			// Drain any pending heartbeat channel readers so close is safe.
			if d.DeregisterUpstreamAgent(agentID, grpcStream, context.Canceled) {
				d.publishDeparture(agentID, grpcStream)
			}
			d.internalAgents.Delete(agentID)
			receiveDone.Wait()
		})
	}
}

// IsInternalAgent reports whether agentID is relay plumbing: the relay's own
// agent (on any pod) or an agent RegisterInternalAgent registered.
// User-facing agent lists and counts leave it out; A2A routing does not.
func (d *Dispatcher) IsInternalAgent(agentID string) bool {
	if agentID == rpcclient.RelayAgentID {
		return true
	}
	if d == nil {
		return false
	}
	_, ok := d.internalAgents.Load(agentID)
	return ok
}
