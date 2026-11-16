// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"errors"
	"sync"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// errAccountRequired is a budgeted leg's error past its cut.
var errAccountRequired = errors.New(nf1anon.Msg)

// budgeted is out, the onward leg of an nf1 session to agentID, counted
// against an anonymous agent's leg budget and its source's bucket (nfltr
// docs/decisions/nf1-anon-2026-11-16.md section 2, spec/NF1Anon.tla
// Relay, Cut, TimeCut): past either, or past the leg's time limit, both
// legs are aborted with ERROR_REASON_ACCOUNT_REQUIRED. An account's leg
// (leg nil) is out itself: never budgeted, spliced as before. A budgeted
// leg hides out's concrete type, so the join copies through it.
func budgeted(in, out peerlink.Session, leg *nf1anon.Leg) peerlink.Session {
	if leg == nil {
		return out
	}
	b := &budget{in: in, out: out, leg: leg}
	b.timer = time.AfterFunc(leg.Time, func() {
		if leg.TimeUp() {
			b.cut("time")
		}
	})
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
func (b *budget) cut(why string) {
	b.once.Do(func() {
		b.timer.Stop()
		metrics.RecordConnectSession(true, "nf1_anon_cut_"+why)
		a := &link.Abort{Code: dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_ACCOUNT_REQUIRED), Msg: nf1anon.Msg}
		abort(b.out, a)
		abort(b.in, a)
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
		b.cut("bytes")
	}
	return fit
}

// read is out's Read, toward the client.
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
