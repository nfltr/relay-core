// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package relay — internal_agent_loopback.go
//
// In-process AgentTunnel + Streamer adapter for a relay-owned orchestrator
// (option A of the inner-LLM drawer fix, docs/architecture/relay-owned-orchestrator.md).
//
// The relay-internal orchestrator needs an rpcclient.Agent that can issue
// A2A calls to workers connected to the same relay. A real network dial
// would require auth + TLS setup just to talk to the same process; instead
// this adapter wires a paired-channel duplex between:
//   - dispatcher side: implements relay.Streamer
//   - agent side: implements rpcclient.AgentTunnel
//
// Frames the dispatcher Sends are visible to the agent's Recv. Frames the
// agent Sends are visible to the dispatcher's Recv. Each side has an
// independent buffered channel so neither side blocks the other.
//
// Lifecycle: a single context.Context owns both halves; cancelling it
// terminates Recv on both sides cleanly with ctx.Err().
package session

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nfltr/relay/proto/pb"
)

// InternalAgentLoopback is a single shared duplex between the dispatcher's
// upstreams map and an in-process rpcclient.Agent. Both halves are exposed
// via accessor methods so the caller can register one with the dispatcher
// and pass the other to rpcclient.NewAgent.
type InternalAgentLoopback struct {
	ctx     context.Context
	cancel  context.CancelFunc
	agentID string

	// dispatcherToAgent carries frames originated by the dispatcher
	// (via dispatcherSide.Send) for delivery to the agent
	// (via agentSide.Recv).
	dispatcherToAgent chan *pb.TunnelFrame

	// agentToDispatcher carries frames originated by the agent
	// (via agentSide.Send) for delivery to the dispatcher
	// (via dispatcherSide.Recv).
	agentToDispatcher chan *pb.TunnelFrame

	closeOnce sync.Once
}

const internalAgentLoopbackBuffer = 256

var errInternalAgentLoopbackBackpressure = errors.New("internal agent loopback backpressure")

// NewInternalAgentLoopback constructs a paired duplex. agentID labels the
// agent half so the dispatcher's upstreams map can index it. ctx scopes
// the lifetime of both halves; cancelling it (or calling Close) ends
// Recv on both sides.
func NewInternalAgentLoopback(parent context.Context, agentID string) *InternalAgentLoopback {
	ctx, cancel := context.WithCancel(parent) // #nosec G118 -- cancel is kept in the struct and called by Close
	return &InternalAgentLoopback{
		ctx:               ctx,
		cancel:            cancel,
		agentID:           agentID,
		dispatcherToAgent: make(chan *pb.TunnelFrame, internalAgentLoopbackBuffer),
		agentToDispatcher: make(chan *pb.TunnelFrame, internalAgentLoopbackBuffer),
	}
}

// AgentID returns the stable agent identifier carried by the loopback.
func (l *InternalAgentLoopback) AgentID() string { return l.agentID }

// Close terminates both halves. Idempotent.
func (l *InternalAgentLoopback) Close() {
	l.closeOnce.Do(l.cancel)
}

// DispatcherSide returns the relay.Streamer the dispatcher should register in
// its upstreams map (Send delivers TO the agent; Recv reads FROM the agent).
func (l *InternalAgentLoopback) DispatcherSide() Streamer {
	return &internalAgentLoopbackDispatcherSide{loop: l}
}

// AgentSide returns the rpcclient.AgentTunnel-compatible side the in-process
// rpcclient.Agent should be constructed against (Send delivers TO the
// dispatcher; Recv reads FROM the dispatcher).
func (l *InternalAgentLoopback) AgentSide() AgentTunnelSide {
	return &internalAgentLoopbackAgentSide{loop: l}
}

// AgentTunnelSide is the agent-half interface — identical in shape to
// rpcclient.AgentTunnel but declared here to avoid an import cycle. The
// agent half is satisfied by *internalAgentLoopbackAgentSide and can be
// passed to rpcclient.NewAgent via an adapter constructed in cmd/server
// (where the rpcclient import is allowed).
type AgentTunnelSide interface {
	Send(*pb.TunnelFrame) error
	Recv() (*pb.TunnelFrame, error)
}

// ── Dispatcher-side ────────────────────────────────────────────────────────

type internalAgentLoopbackDispatcherSide struct {
	loop *InternalAgentLoopback
	ctx  context.Context
}

func (s *internalAgentLoopbackDispatcherSide) WithContext(ctx context.Context) Streamer {
	if ctx == nil {
		ctx = context.Background()
	}
	return &internalAgentLoopbackDispatcherSide{loop: s.loop, ctx: ctx}
}

func (s *internalAgentLoopbackDispatcherSide) done() <-chan struct{} {
	if s.ctx != nil {
		return s.ctx.Done()
	}
	return s.loop.ctx.Done()
}

func (s *internalAgentLoopbackDispatcherSide) err() error {
	if s.ctx != nil && s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	return s.loop.ctx.Err()
}

func (s *internalAgentLoopbackDispatcherSide) Send(msg *pb.TunnelFrame) error {
	select {
	case <-s.done():
		return s.err()
	default:
	}
	select {
	case s.loop.dispatcherToAgent <- msg:
		return nil
	case <-s.done():
		return s.err()
	default:
		return fmt.Errorf("%w: queue_depth=%d capacity=%d", errInternalAgentLoopbackBackpressure, len(s.loop.dispatcherToAgent), cap(s.loop.dispatcherToAgent))
	}
}

func (s *internalAgentLoopbackDispatcherSide) Recv() (*pb.TunnelFrame, error) {
	select {
	case msg := <-s.loop.agentToDispatcher:
		return msg, nil
	case <-s.done():
		return nil, s.err()
	}
}

func (s *internalAgentLoopbackDispatcherSide) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}
	return s.loop.ctx
}

// ── Agent-side ────────────────────────────────────────────────────────────

type internalAgentLoopbackAgentSide struct {
	loop *InternalAgentLoopback
}

func (s *internalAgentLoopbackAgentSide) Send(msg *pb.TunnelFrame) error {
	select {
	case s.loop.agentToDispatcher <- msg:
		return nil
	case <-s.loop.ctx.Done():
		return s.loop.ctx.Err()
	}
}

func (s *internalAgentLoopbackAgentSide) Recv() (*pb.TunnelFrame, error) {
	select {
	case msg := <-s.loop.dispatcherToAgent:
		return msg, nil
	case <-s.loop.ctx.Done():
		return nil, s.loop.ctx.Err()
	}
}
