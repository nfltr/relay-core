// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import "context"

// LongBodyBytes is the body size past which a request or a response is
// long-lived: a transfer that may take minutes is held, a small one that
// ends in a round trip only carries its principal.
const LongBodyBytes = 1 << 20

// carried is a context carrying a principal its Authenticator has not held
// (Carry): a short request's, which a hold would cost a cancel context, a
// map entry and maybe a timer for nothing. HoldCarried holds it once the
// request turns out long-lived.
type carried struct {
	context.Context
	p Principal
	a *Authenticator
}

type carriedKey struct{}

// Value implements context.Context.
func (c *carried) Value(key any) any {
	switch key {
	case principalKey{}:
		return c.p
	case carriedKey{}:
		return c
	}
	return c.Context.Value(key)
}

// Carry returns ctx carrying p (FromContext) without holding it, for a
// connection that ends soon: one allocation. A revocation or p's expiry
// does not end it until HoldCarried holds it.
func (a *Authenticator) Carry(ctx context.Context, p Principal) context.Context {
	return &carried{Context: ctx, p: p, a: a}
}

// HoldCarried holds, from now until its Release, the connection ctx serves
// when ctx carries a principal Carry gave it and no hold covers it yet: the
// Held is ctx's child and ends when the credential is revoked or expires.
// ok is false (nothing to release) when ctx carries no such principal.
func HoldCarried(ctx context.Context) (h *Held, ok bool) {
	c, ok := ctx.Value(carriedKey{}).(*carried)
	if !ok {
		return nil, false
	}
	return c.a.Hold(ctx, c.p), true
}
