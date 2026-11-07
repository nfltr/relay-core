// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"errors"
	"testing"
)

// hooked runs after its provider's lookup, before Authenticate returns:
// where a revocation can land between the read and the hold.
type hooked struct {
	Provider
	after func()
}

func (h hooked) Authenticate(ctx context.Context, c Credential) (Principal, error) {
	p, err := h.Provider.Authenticate(ctx, c)
	if h.after != nil {
		h.after()
	}
	return p, err
}

// TestRevocationDuringAuthenticate: a revocation that lands after
// Authenticate read the provider (spec/README.md finding 5 in
// nfltr/relay-server) or after it returned, before the hold, cuts the
// connection at Hold and leaves no cached answer: the key's next
// connection is asked of the provider again, which refuses it.
func TestRevocationDuringAuthenticate(t *testing.T) {
	for _, during := range []bool{true, false} {
		name := map[bool]string{true: "after the provider read", false: "after Authenticate"}[during]
		t.Run(name, func(t *testing.T) {
			f := &fakeProvider{name: "keys", tokens: map[string]Principal{
				"k": {Owner: "o", Subject: "agent", CacheTTL: 1 << 40},
			}}
			var a *Authenticator
			revoke := func() {
				delete(f.tokens, "k") // the store refuses it from now on
				a.Revoke(Key("keys", "agent"))
			}
			h := hooked{Provider: f}
			if during {
				h.after = revoke
			}
			a = NewAuthenticator("keys", h)
			p, err := a.Authenticate(t.Context(), Credential{Token: "k"})
			if err != nil {
				t.Fatal(err)
			}
			if !during {
				revoke()
			}
			held := a.Hold(t.Context(), p)
			defer held.Release()
			if !errors.Is(context.Cause(held), ErrRevoked) {
				t.Errorf("held after the revocation: cause %v, want %v", context.Cause(held), ErrRevoked)
			}
			if n := a.Held("keys:agent"); n != 0 {
				t.Errorf("%d connections held", n)
			}
			if p, err := a.Authenticate(t.Context(), Credential{Token: "k"}); err == nil {
				t.Errorf("the revoked key authenticated again (a cached answer): %+v", p)
			}
		})
	}
}

// TestRevocationKeepsOtherKeysAndLaterReads: a revocation cuts only
// principals read before it, of its key: another key's and a later read of
// the same subject (a key minted again for it) are held.
func TestRevocationKeepsOtherKeysAndLaterReads(t *testing.T) {
	f := &fakeProvider{name: "keys", tokens: map[string]Principal{
		"k": {Owner: "o", Subject: "agent"},
		"j": {Owner: "o", Subject: "other"},
	}}
	a := NewAuthenticator("keys", f)
	pj, _ := a.Authenticate(t.Context(), Credential{Token: "j"})
	a.Revoke(Key("keys", "agent"))
	pk, err := a.Authenticate(t.Context(), Credential{Token: "k"})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []Principal{pj, pk} {
		h := a.Hold(t.Context(), p)
		if h.Err() != nil {
			t.Fatalf("%s: cut: %v", p.Key(), context.Cause(h))
		}
		h.Release()
	}
}

// TestRevocationObservedLeavesNoCachedAnswer: whoever sees a revocation's
// cut (its held connection ending) finds no cached answer left: the cut
// comes after the cache is cleared, so the key's next connection is asked
// of the provider, which refuses it (relay-server
// TestRevocationReachesEveryReplica saw the cut, then a cached accept).
func TestRevocationObservedLeavesNoCachedAnswer(t *testing.T) {
	for i := 0; i < 300; i++ {
		f := &fakeProvider{name: "keys", tokens: map[string]Principal{
			"k": {Owner: "o", Subject: "agent", CacheTTL: 1 << 40},
		}}
		a := NewAuthenticator("keys", f)
		p, err := a.Authenticate(t.Context(), Credential{Token: "k"})
		if err != nil {
			t.Fatal(err)
		}
		held := a.Hold(t.Context(), p)
		again := make(chan error, 1)
		context.AfterFunc(held, func() {
			_, err := a.Authenticate(context.Background(), Credential{Token: "k"})
			again <- err
		})
		delete(f.tokens, "k") // the store refuses it from now on
		a.Revoke(Key("keys", "agent"))
		if err := <-again; err == nil {
			t.Fatalf("run %d: the key authenticated from the cache after its cut was seen", i)
		}
		held.Release()
	}
}
