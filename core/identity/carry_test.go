// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCarryHoldsOnlyWhenAsked: a carried principal is read like a held
// one but holds nothing; HoldCarried holds it from then on (a revocation
// ends the hold), once, and only on a carried context.
func TestCarryHoldsOnlyWhenAsked(t *testing.T) {
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{"t": {Owner: "o", Subject: "s", Names: AnyName}}}
	a := NewAuthenticator("accounts", prov)
	p, err := a.Authenticate(t.Context(), Credential{Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := a.Carry(t.Context(), p)
	if got, _ := FromContext(ctx); got.Owner != "o" || a.Held(p.Key()) != 0 {
		t.Fatalf("carried: principal %+v, held %d", got, a.Held(p.Key()))
	}
	if _, ok := HoldCarried(t.Context()); ok {
		t.Fatal("held a context that carries no principal")
	}
	h, ok := HoldCarried(context.WithValue(ctx, struct{}{}, 1)) // a child of the carried context
	if !ok || a.Held(p.Key()) != 1 {
		t.Fatalf("HoldCarried: %v, held %d", ok, a.Held(p.Key()))
	}
	defer h.Release()
	if _, again := HoldCarried(h); again {
		t.Fatal("held a held context twice")
	}
	if n := a.Revoke(p.Key()); n != 1 || !errors.Is(context.Cause(h), ErrRevoked) || ctx.Err() != nil {
		t.Fatalf("revoke cut %d, hold %v, the carried context %v", n, context.Cause(h), ctx.Err())
	}
}

// TestHoldCarriedExpires: a carried principal past its NotAfter ends at
// once when held.
func TestHoldCarriedExpires(t *testing.T) {
	a := NewAuthenticator("accounts")
	h, _ := HoldCarried(a.Carry(t.Context(), Principal{Provider: "accounts", Subject: "s", NotAfter: time.Now().Add(-time.Second)}))
	defer h.Release()
	select {
	case <-h.Done():
		if !errors.Is(context.Cause(h), ErrExpired) {
			t.Fatalf("cause %v", context.Cause(h))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an expired principal's hold did not end")
	}
}
