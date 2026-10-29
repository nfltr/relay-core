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
// p.NotAfter (ErrExpired) unless that is zero.
func (l *Live) hold(ctx context.Context, subject string, p Principal) *Held {
	ctx, cancel := context.WithCancelCause(ctx)
	h := &Held{Context: ctx, p: p, cancel: cancel, live: l}
	if !p.NotAfter.IsZero() {
		h.expiry = time.AfterFunc(time.Until(p.NotAfter), func() { cancel(ErrExpired) })
	}
	l.mu.Lock()
	if l.held == nil {
		l.held = make(map[*Held]string)
	}
	l.held[h] = subject
	l.mu.Unlock()
	return h
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

// Cut ends every connection subject holds with cause and returns how many.
func (l *Live) Cut(subject string, cause error) int {
	var cut []*Held
	l.mu.Lock()
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
