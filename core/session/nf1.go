// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// NF1NotServed is the one answer to an open of a capability address that
// no live agent serves here: unknown, offline, every server's dial failed,
// or a relay without the shared directory. It names no agent, so a caller
// learns nothing about which rendezvous ids exist (nfltr
// docs/decisions/nf1-rendezvous-2026-11-13.md section 4).
const NF1NotServed = "no nf1 service at this address"

func nf1NotServed() *Refusal {
	return &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, Msg: NF1NotServed}
}

// openNF1 is Open of a capability address: the live agents whose records
// declare t.NF1 (directory.NF1Index), each a holder of the address's
// server key (agent_services.go), newest claim first, the next when a dial
// finds the agent gone, within the route budget (spec/NF1Claim.tla Decide,
// Dial; nfltr docs/decisions/nf1-risks-2026-11-14.md). The session is that
// agent's service of the declaration, end to end encrypted, as an
// anonymous caller (no identity reaches the agent). The agent's owner pays
// (the frontend meters the session). A refusal never names an agent; a
// DialError (the last dial's, when every one failed) does, for the
// frontend's log only.
func (c *Core) openNF1(ctx context.Context, t Target) (Opened, error) {
	idx, ok := c.Dispatcher.IPInventory.(directory.NF1Index)
	if !ok {
		return Opened{}, nf1NotServed()
	}
	budget := c.routeBudget
	if budget == 0 {
		budget = routeBudget
	}
	route, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	dirUnavailable := &Refusal{Reason: relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE, Msg: "the relay's directory is unavailable"}
	agents, err := idx.NF1Agents(route, t.NF1)
	if err != nil {
		return Opened{}, dirUnavailable
	}
	var last error = nf1NotServed()
	for i, agentID := range agents {
		services, connected := c.Dispatcher.AgentServices(agentID)
		decl, ok := agentservices.FindNF1(services, t.NF1)
		if !connected || !ok {
			continue // gone since the read: the next
		}
		release := func() {}
		if t.Admit != nil {
			if release, err = t.Admit(agentID); err != nil {
				return Opened{}, err
			}
		}
		retries, interval := c.DialPolicy()
		if i < len(agents)-1 {
			retries = 1 // another server is left: a crashed one's record must not spend the budget
		}
		ep := TCPEndpoint{AgentID: agentID, TargetAddr: decl.Target(), E2EE: true, Answer: t.Answer}
		s, done, err := c.TCPDispatcher.OnwardWithRetry(route, ep, retries, interval)
		if err == nil {
			return Opened{Session: nf1Leg(s), AgentID: agentID, Release: func() {
				if done != nil {
					done()
				}
				release()
			}}, nil
		}
		release()
		dial := &DialError{AgentID: agentID, TargetAddr: decl.Target(), Err: fmt.Errorf("nf1: %w", err)}
		switch {
		case errors.Is(dial, ErrDirectoryUnavailable): // not the agent's failure: never "not served"
			return Opened{}, dirUnavailable
		case errors.Is(dial, ErrDraining) || route.Err() != nil:
			return Opened{}, dial // no other server would fare better
		}
		c.Dispatcher.Logger.Debug("nf1: dial failed, trying the next server", "agent_id", agentID, "error", err)
		last = dial
	}
	return Opened{}, last
}

// nativeLeg is an onward leg that answers opens itself (a link or quiclink
// session) with every end method the joint uses (peerlink: EndAcker,
// endSyncer, receiptSyncer).
type nativeLeg interface {
	peerlink.ChainSession
	HoldEndAck()
	AckEnd()
	CloseWriteSync() error
	CloseWriteSyncReceived(received func()) error
}

// nf1Leg is an nf1 open's leg to its agent whose answer names no agent:
// the agent gone before it answered (its link ended or drains at its
// stop: a link refusal of its leg, or the connection's end) is the one
// answer NF1NotServed, as for an unknown address; the agent's own
// refusal passes on (spec/NF1Claim.tla Answer, OneAnswer, mutant
// lost-answer; lane PIPERESET2: TestNF1CapsAndNotServed read "relay
// unreachable"). A leg without native answers (an in-band one) is s.
func nf1Leg(s peerlink.Session) peerlink.Session {
	if l, ok := s.(nativeLeg); ok && l.Answers() {
		return answerNF1{l}
	}
	return s
}

type answerNF1 struct{ nativeLeg }

func (l answerNF1) Answered(ctx context.Context) error {
	err := l.nativeLeg.Answered(ctx)
	var a *link.Abort
	if err == nil || ctx.Err() != nil || errors.As(err, &a) && a.Code >= link.CodeApp {
		return err
	}
	r := nf1NotServed()
	return dataplane.Refusal(r.Reason, r.Msg)
}

// Done is the leg's Done channel (peerlink's doneOf), nil when it has none.
func (l answerNF1) Done() <-chan struct{} {
	if d, ok := l.nativeLeg.(interface{ Done() <-chan struct{} }); ok {
		return d.Done()
	}
	return nil
}
