// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"sync"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// errAccountRequired is a budgeted leg's error past its cut: the cut's
// abort itself, so the join passes the typed reason on.
var errAccountRequired = &link.Abort{Code: dataplane.AbortCode(nf1anon.Reason), Msg: nf1anon.Msg}

// budgeted is out, the onward leg of an nf1 session to agentID, counted
// against an anonymous agent's leg budget and its source's bucket (nfltr
// docs/decisions/nf1-anon-2026-11-16.md section 2, spec/NF1Anon.tla
// Relay, Cut, TimeCut): past either, or past the leg's time limit, both
// legs are aborted with ERROR_REASON_ACCOUNT_REQUIRED. An account's leg
// (leg nil) is out itself: never budgeted, spliced as before. A budgeted
// leg hides out's concrete type, so the join copies through it, but passes
// its end on (endLeg: lane QUICEND, TestSessionWrappersPassTheEndOn).
func budgeted(in, out peerlink.Session, leg *nf1anon.Leg) peerlink.Session {
	if leg == nil {
		return out
	}
	b := &budget{in: in, out: out, leg: leg}
	b.timer = time.AfterFunc(leg.Time, func() {
		if leg.TimeUp() {
			b.cut()
		}
	})
	if e, ok := out.(endLeg); ok {
		return &budgetEndLeg{endLeg: e, b: b}
	}
	if c, ok := out.(peerlink.ChainSession); ok {
		return &budgetChainLeg{ChainSession: c, b: b}
	}
	return &budgetLeg{Session: out, b: b}
}

// budget is one budgeted leg's state.
type budget struct {
	in, out peerlink.Session
	leg     *nf1anon.Leg
	timer   *time.Timer
	once    sync.Once
}

// cut aborts both legs with the typed reason, once.
func (b *budget) cut() {
	b.once.Do(func() {
		abort(b.out, errAccountRequired)
		abort(b.in, errAccountRequired)
	})
}

// abort ends s with a, or resets it when it carries no aborts.
func abort(s peerlink.Session, a *link.Abort) {
	if c, ok := s.(interface{ AbortWith(*link.Abort) error }); ok {
		_ = c.AbortWith(a)
		return
	}
	_ = s.Reset()
}

// take counts n bytes the leg is to carry: how many it may, cutting it when
// that is fewer.
func (b *budget) take(n int) int {
	fit := b.leg.Take(n)
	if fit < n {
		b.cut()
	}
	return fit
}

// read is out's Read, toward the client. Once cut, out reads and writes
// the cut's abort (link: a session aborted here never reads a clean end),
// which the join passes on (spec/NF1Anon.tla JoinEnd, mutant
// join-clean-end; TestBudgetedLegEndsTyped).
func (b *budget) read(p []byte) (int, error) {
	n, err := b.out.Read(p)
	if n > 0 {
		if fit := b.take(n); fit < n {
			return fit, errAccountRequired
		}
	}
	return n, err
}

// write is out's Write, from the client.
func (b *budget) write(p []byte) (int, error) {
	fit := b.take(len(p))
	n, err := b.out.Write(p[:fit])
	if err == nil && fit < len(p) {
		err = errAccountRequired
	}
	return n, err
}

// close stops the leg's timer; the leg ended (the peers ended it, a
// punch released it, or the cut).
func (b *budget) close() {
	b.timer.Stop()
	b.leg.End()
}

type budgetLeg struct {
	peerlink.Session
	b *budget
}

func (l *budgetLeg) Read(p []byte) (int, error)  { return l.b.read(p) }
func (l *budgetLeg) Write(p []byte) (int, error) { return l.b.write(p) }
func (l *budgetLeg) Close() error                { l.b.close(); return l.Session.Close() }

type budgetChainLeg struct {
	peerlink.ChainSession
	b *budget
}

func (l *budgetChainLeg) Read(p []byte) (int, error)  { return l.b.read(p) }
func (l *budgetChainLeg) Write(p []byte) (int, error) { return l.b.write(p) }
func (l *budgetChainLeg) Close() error                { l.b.close(); return l.ChainSession.Close() }

// endLeg is a link or quiclink session: every end method a joint uses
// (peerlink: EndAcker, endSyncer, receiptSyncer). Hidden behind
// budgetChainLeg, an anonymous nf1 leg's joint half-closed it without
// waiting for the far end and never held its end (lane QUICEND).
type endLeg interface {
	peerlink.ChainSession
	peerlink.EndAcker
	CloseWriteSync() error
	CloseWriteSyncReceived(received func()) error
}

type budgetEndLeg struct {
	endLeg
	b *budget
}

func (l *budgetEndLeg) Read(p []byte) (int, error)  { return l.b.read(p) }
func (l *budgetEndLeg) Write(p []byte) (int, error) { return l.b.write(p) }
func (l *budgetEndLeg) Close() error                { l.b.close(); return l.endLeg.Close() }

// Done is the leg's Done channel (peerlink's doneOf), nil when it has none.
func (l *budgetEndLeg) Done() <-chan struct{} {
	if d, ok := l.endLeg.(interface{ Done() <-chan struct{} }); ok {
		return d.Done()
	}
	return nil
}
