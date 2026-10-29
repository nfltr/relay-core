// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// Open answers a frontend with typed errors: what Reach refuses keeps its
// reason and is refused, offline or an unknown service; a dial that finds
// no agent is offline; one a draining replica cuts is draining.
func TestOpenAnswersAreTyped(t *testing.T) {
	td := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, directory.NewNoOpInventory())
	caller := identity.Principal{Provider: "keys", Owner: "keys:alice", Subject: "alice", AgentID: "alice"}
	var asked identity.Principal
	c := &Core{TCPDispatcher: td, DialPolicy: func() (int, time.Duration) { return 1, time.Millisecond }}
	c.Reach = func(_ context.Context, p identity.Principal, tg Target) (string, string, error) {
		asked = p
		switch tg.Agent {
		case "nobody":
			return "", "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, Msg: "agent not found", AgentID: tg.Agent}
		case "gone":
			return "", "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, Msg: "agent gone is not connected", AgentID: tg.Agent}
		case "nosvc":
			return "", "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE, Msg: "no tcp service", AgentID: tg.Agent}
		}
		return tg.Agent, "localhost:22", nil
	}
	for _, tc := range []struct {
		agent string
		want  error
	}{
		{"nobody", ErrRefused},
		{"gone", ErrOffline},
		{"nosvc", ErrUnknownService},
		{"alice.laptop", ErrOffline}, // passes Reach, no agent connected anywhere
	} {
		_, err := c.Open(context.Background(), caller, Target{Agent: tc.agent, Service: "22"})
		if !errors.Is(err, tc.want) {
			t.Errorf("Open(%s) = %v, want %v", tc.agent, err, tc.want)
		}
		if asked.Key() != caller.Key() || asked.AgentID != caller.AgentID {
			t.Errorf("Open(%s): Reach saw %+v, want the caller", tc.agent, asked)
		}
	}
	var dial *DialError
	if _, err := c.Open(context.Background(), caller, Target{Agent: "alice.laptop", Service: "22"}); !errors.As(err, &dial) || dial.AgentID != "alice.laptop" || dial.TargetAddr != "localhost:22" {
		t.Errorf("a dial with no agent = %v, want a DialError naming the agent and target", err)
	}

	off := &Core{}
	if _, err := off.Open(context.Background(), caller, Target{Agent: "alice.laptop", Service: "22"}); !errors.Is(err, ErrRefused) {
		t.Errorf("Open without TCP = %v, want refused", err)
	}

	for err, want := range map[error]error{
		fmt.Errorf("onward: %w", peerlink.ErrDraining): ErrDraining,
		errNoAnswers("a"):       ErrRefused,
		errors.New("no stream"): ErrOffline,
	} {
		if got := (&DialError{Err: err}); !errors.Is(got, want) {
			t.Errorf("DialError{%v} is not %v", err, want)
		}
	}
}

// Only a held open (Target.Hold: a client's, the connect sidecar's) waits
// for an offline agent; a route that tries the next agent on an offline
// answer (service_route.go) gets it at once.
func TestOpenHoldsOnlyAHeldOpen(t *testing.T) {
	c := &Core{TCPDispatcher: NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, directory.NewNoOpInventory())}
	c.Reach = func(_ context.Context, _ identity.Principal, tg Target) (string, string, error) {
		return "", "", &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, Msg: "offline", AgentID: tg.Agent}
	}
	for _, hold := range []bool{false, true} {
		start := time.Now()
		_, err := c.Open(context.Background(), identity.Principal{}, Target{Agent: "gone", Service: "22", Hold: hold})
		took := time.Since(start)
		if !errors.Is(err, ErrOffline) || hold != (took >= offlineHold) {
			t.Errorf("Hold %v: %v after %v", hold, err, took)
		}
	}
}

// TestOpenAnswersAStalledRouteTyped (kind soak 2026-10-26: refusal probes
// ended by their own 10 s deadline while a stalled host's directory reads
// waited): Open's own routing is bounded, and a route that runs out of it
// is refused DIRECTORY_UNAVAILABLE before the client's bound.
func TestOpenAnswersAStalledRouteTyped(t *testing.T) {
	c := &Core{TCPDispatcher: NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, directory.NewNoOpInventory()), routeBudget: 100 * time.Millisecond}
	c.Reach = func(ctx context.Context, _ identity.Principal, _ Target) (string, string, error) {
		<-ctx.Done() // a directory read that waits for as long as it is let
		return "", "", fmt.Errorf("directory: %w", ctx.Err())
	}
	client, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err := c.Open(client, identity.Principal{}, Target{Agent: "alice.laptop", Service: "22", Hold: true})
	var r *Refusal
	if !errors.As(err, &r) || r.Reason != relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE || client.Err() != nil {
		t.Fatalf("a stalled route: %v after %v, want a DIRECTORY_UNAVAILABLE refusal within the route budget", err, time.Since(start))
	}
	// The client's own bound, shorter than the budget, stays its own.
	short, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	if _, err := c.Open(short, identity.Principal{}, Target{Agent: "alice.laptop", Service: "22"}); errors.As(err, &r) {
		t.Fatalf("a client that gave up first got the relay's refusal %v", err)
	}
}
