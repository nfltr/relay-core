// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package identity is the relay core's identity seam: what a connection's
// credential is worth (a principal), and the core's hold on every live
// connection a credential opened, so that a revocation or an expiry ends
// them wherever they are.
package identity

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrRevoked is the cancel cause of a connection whose credential was
// revoked; ErrExpired of one whose credential expired.
var (
	ErrRevoked = errors.New("credential revoked")
	ErrExpired = errors.New("credential expired")
)

// Live holds the live connections of each credential subject: a
// transport holds every connection it authenticated for as long as it
// serves it (Authenticator.Hold); Cut ends them all with a cause. The zero
// value is ready to use.
type Live struct {
	mu   sync.Mutex
	held map[*Held]string // connection -> subject; Cut, rare, walks it
	// gen counts Cuts; cut[subject] is the gen of subject's last Cut, so a
	// connection whose principal was read before it is cut at hold.
	// ponytail: cut keeps one entry per subject ever cut (revocations are
	// rare); bound it by age if a relay ever revokes millions of keys.
	gen uint64
	cut map[string]uint64
}

// Held is one held connection: its context, which carries the principal
// that opened it (FromContext) and ends when the credential is revoked or
// expires. Release it when the connection ends.
type Held struct {
	context.Context
	p      Principal
	cancel context.CancelCauseFunc
	expiry *time.Timer
	live   *Live
}

// Value implements context.Context.
func (h *Held) Value(key any) any {
	switch key {
	case principalKey{}:
		return h.p
	case carriedKey{}:
		return nil // held already: HoldCarried does not hold it twice
	}
	return h.Context.Value(key)
}

// hold holds a connection p opened under subject; it also ends at
// p.NotAfter (ErrExpired) unless that is zero. A p read before subject's
// last Cut (Authenticator.Authenticate's generation) is ended at once with
// ErrRevoked: the revocation raced its authentication.
func (l *Live) hold(ctx context.Context, subject string, p Principal) *Held {
	ctx, cancel := context.WithCancelCause(ctx)
	h := &Held{Context: ctx, p: p, cancel: cancel, live: l}
	l.mu.Lock()
	if l.cut[subject] > p.gen {
		l.mu.Unlock()
		cancel(ErrRevoked)
		return h
	}
	if l.held == nil {
		l.held = make(map[*Held]string)
	}
	l.held[h] = subject
	l.mu.Unlock()
	if !p.NotAfter.IsZero() {
		h.expiry = time.AfterFunc(time.Until(p.NotAfter), func() { cancel(ErrExpired) })
	}
	return h
}

// generation is the number of Cuts so far.
func (l *Live) generation() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.gen
}

// cutSince reports whether subject was cut after generation gen.
func (l *Live) cutSince(subject string, gen uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cut[subject] > gen
}

// Release ends the hold: the connection ended.
func (h *Held) Release() {
	l := h.live
	l.mu.Lock()
	delete(l.held, h)
	l.mu.Unlock()
	if h.expiry != nil {
		h.expiry.Stop()
	}
	h.cancel(nil)
}

// Cut ends every connection subject holds with cause and returns how many;
// a hold of a principal read before the Cut ends at once (hold).
func (l *Live) Cut(subject string, cause error) int {
	var cut []*Held
	l.mu.Lock()
	l.gen++
	if l.cut == nil {
		l.cut = make(map[string]uint64)
	}
	l.cut[subject] = l.gen
	for h, s := range l.held {
		if s == subject {
			cut = append(cut, h)
			delete(l.held, h)
		}
	}
	l.mu.Unlock()
	for _, h := range cut {
		h.cancel(cause)
	}
	return len(cut)
}

// Held returns how many connections subject holds.
func (l *Live) Held(subject string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, s := range l.held {
		if s == subject {
			n++
		}
	}
	return n
}
