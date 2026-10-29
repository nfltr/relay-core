// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// Open's answers, typed: a frontend tells them apart with errors.Is and
// relays each in its own protocol.
var (
	// ErrRefused: the caller may not open the session. A target that does
	// not exist gets the same answer, so a caller cannot probe for agents.
	ErrRefused        = errors.New("refused")
	ErrOffline        = errors.New("agent offline")
	ErrUnknownService = errors.New("unknown service")
	ErrDraining       = errors.New("draining")
	// ErrDirectoryUnavailable: the relay could not read where the agent
	// is (directory.ErrUnavailable): its own failure, not the agent's.
	ErrDirectoryUnavailable = errors.New("directory unavailable")
)

// Target is what a caller opens: a service of an agent, as the caller
// names them.
type Target struct {
	Agent   string // the agent, as the caller names it
	Service string // a service the agent declared, or a port
	E2EE    bool   // the caller's TLS ends at the agent, opaque to the relay
	Answer  bool   // the agent answers the open once it reached its target
	// Hold: an offline agent's open is held up to offlineHold for the agent
	// to register (a client's held open: the connect sidecar's); without
	// it the offline answer comes at once (a route that tries the next
	// agent).
	Hold bool
}

// Reach is the access rule and target resolution the composition sets
// (Core.Reach): whether caller may open t, and the agent and address it
// opens. It refuses with a *Refusal.
type Reach func(ctx context.Context, caller identity.Principal, t Target) (agentID, targetAddr string, err error)

// Refusal is an open Reach refused, with the reason and message the
// frontend relays and the agent it names (as resolved so far).
type Refusal struct {
	Reason  relayv1.ErrorReason
	Msg     string
	AgentID string
}

// Error returns the refusal's message.
func (r *Refusal) Error() string { return r.Msg }

// Is classifies the refusal by its reason.
func (r *Refusal) Is(target error) bool {
	switch r.Reason {
	case relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE:
		return target == ErrOffline
	case relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE:
		return target == ErrUnknownService
	}
	return target == ErrRefused
}

// DialError is an open that passed Reach but did not reach its agent: the
// agent is not connected, or does not answer opens (ErrAgentAnswers), or a
// replica on the way drains.
type DialError struct {
	AgentID, TargetAddr string
	Err                 error
}

// Error returns the dial failure's message.
func (e *DialError) Error() string { return e.Err.Error() }

// Unwrap returns the dial failure.
func (e *DialError) Unwrap() error { return e.Err }

// Is classifies the dial failure.
func (e *DialError) Is(target error) bool {
	switch {
	case errors.Is(e.Err, peerlink.ErrDraining) || errors.Is(e.Err, link.ErrGoAway):
		return target == ErrDraining
	case errors.Is(e.Err, ErrAgentAnswers):
		return target == ErrRefused
	case errors.Is(e.Err, directory.ErrUnavailable):
		return target == ErrDirectoryUnavailable
	}
	return target == ErrOffline
}

// Opened is an open session to an agent: Release ends its accounting once
// the session is done.
type Opened struct {
	peerlink.Session
	AgentID string
	Release func()
}

// Open opens a byte session to t on behalf of caller: Reach decides whether
// caller may and resolves the agent and target address, the TCPDispatcher
// opens it on the agent, here or through the replica that holds it,
// retrying a dial as DialPolicy says. It answers with a *Refusal or a
// *DialError (ErrRefused, ErrOffline, ErrUnknownService, ErrDraining).
func (c *Core) Open(ctx context.Context, caller identity.Principal, t Target) (Opened, error) {
	if c.TCPDispatcher == nil || c.Reach == nil {
		return Opened{}, &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_FEATURE_UNSUPPORTED, Msg: "TCP tunneling is not enabled (set tcp.enabled=true)", AgentID: t.Agent}
	}
	budget := c.routeBudget
	if budget == 0 {
		budget = routeBudget
	}
	route, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	agentID, addr, err := c.reachHeld(route, caller, t)
	if err == nil {
		retries, interval := c.DialPolicy()
		ep := TCPEndpoint{AgentID: agentID, TargetAddr: addr, E2EE: t.E2EE, Answer: t.Answer}
		var s peerlink.Session
		var release func()
		if s, release, err = c.TCPDispatcher.OnwardWithRetry(route, ep, retries, interval); err == nil {
			return Opened{Session: s, AgentID: agentID, Release: release}, nil
		}
		err = &DialError{AgentID: agentID, TargetAddr: addr, Err: err}
	}
	if ctx.Err() == nil && errors.Is(route.Err(), context.DeadlineExceeded) {
		return Opened{}, &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, AgentID: t.Agent,
			Msg: fmt.Sprintf("the relay could not route the session within %v", budget)}
	}
	return Opened{}, err
}

// routeBudget bounds Open's own routing (Reach, the directory, the replica
// link; not the agent's answer, which comes after): a stalled directory is
// answered with a typed refusal before a client's own open bound (10 s,
// the relay client's) ends the session untyped (kind soak 2026-10-26: 9
// refusal probes ended by their own deadline while every directory read of
// a stalled host waited). Core.routeBudget overrides it (tests).
const routeBudget = 5 * time.Second

// offlineHold is how long Open holds an open whose agent is offline before
// it answers ErrOffline: an agent that connects meanwhile gets the session.
// It stays well below a client's open timeout (the connect sidecar's
// --open-timeout, which asks again until its own bound): a client whose
// bound cuts the first held open gets no typed offline answer.
const offlineHold = 500 * time.Millisecond

// offlineRecheck is how often a held open asks Reach again without a
// registration here: an agent that connects to another replica.
// ponytail: a directory read per held open every 250 ms; a cross-replica
// registration signal if multi-replica relays hold many opens.
const offlineRecheck = 250 * time.Millisecond

// reachHeld is Reach, holding an offline answer of a t.Hold open up to
// offlineHold (and ctx): it asks again as soon as an agent's TCP stream registers here, and
// every offlineRecheck.
func (c *Core) reachHeld(ctx context.Context, caller identity.Principal, t Target) (string, string, error) {
	if !t.Hold {
		return c.Reach(ctx, caller, t)
	}
	hold := time.NewTimer(offlineHold)
	defer hold.Stop()
	for {
		registered := c.TCPDispatcher.nextRegistration() // before Reach: none is missed
		agentID, addr, err := c.Reach(ctx, caller, t)
		if !errors.Is(err, ErrOffline) {
			return agentID, addr, err
		}
		recheck := time.NewTimer(offlineRecheck)
		select {
		case <-registered:
		case <-recheck.C:
		case <-hold.C:
			return agentID, addr, err
		case <-ctx.Done():
			recheck.Stop()
			return agentID, addr, err
		}
		recheck.Stop()
	}
}

// DialPolicy is how many times, how far apart, Open dials an agent.
type DialPolicy func() (retries int, interval time.Duration)
