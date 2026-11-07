// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRevokeCauseReachesEveryHold: a cut with a cause (a prepaid account
// whose balance is exhausted) ends the held connections with that cause,
// and so does the hold of a principal read before the cut (the race
// Live.hold closes): the transports answer each typed, never as a revoked
// key the client would stop on.
func TestRevokeCauseReachesEveryHold(t *testing.T) {
	prov := &fakeProvider{name: "accounts", tokens: map[string]Principal{
		"t": {Owner: "o", Subject: "s", CacheTTL: time.Minute},
	}}
	a := NewAuthenticator("accounts", prov)
	p, err := a.Authenticate(t.Context(), Credential{Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	held := a.Hold(t.Context(), p)
	defer held.Release()
	if n := a.RevokeCause(p.Key(), ErrBalanceExhausted); n != 1 || !errors.Is(context.Cause(held), ErrBalanceExhausted) {
		t.Fatalf("cut %d, cause %v; want 1, ErrBalanceExhausted", n, context.Cause(held))
	}
	raced := a.Hold(t.Context(), p) // read before the cut
	defer raced.Release()
	if !errors.Is(context.Cause(raced), ErrBalanceExhausted) {
		t.Fatalf("a hold read before the cut ends with %v, want ErrBalanceExhausted", context.Cause(raced))
	}
	if !Ended(ErrBalanceExhausted) || !Ended(ErrRevoked) || !Ended(ErrExpired) || Ended(context.Canceled) || Ended(nil) {
		t.Fatal("Ended: a credential's cut or expiry, nothing else")
	}
}
