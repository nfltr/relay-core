// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"context"
	"errors"
	"testing"
)

func TestLiveCutEndsOnlyTheSubjectsConnections(t *testing.T) {
	var l Live
	a1 := l.hold(t.Context(), "alice", Principal{})
	a2 := l.hold(t.Context(), "alice", Principal{})
	b := l.hold(t.Context(), "bob", Principal{})
	defer b.Release()
	if n := l.Held("alice"); n != 2 {
		t.Fatalf("alice holds %d, want 2", n)
	}
	if n := l.Cut("alice", ErrRevoked); n != 2 {
		t.Fatalf("cut %d, want 2", n)
	}
	for _, ctx := range []context.Context{a1, a2} {
		if !errors.Is(context.Cause(ctx), ErrRevoked) {
			t.Fatalf("alice's connection: cause %v, want ErrRevoked", context.Cause(ctx))
		}
	}
	if b.Err() != nil || l.Held("bob") != 1 {
		t.Fatalf("bob's connection ended with alice's: %v", b.Err())
	}
	a1.Release() // a release after the cut is harmless
	if l.Cut("alice", ErrRevoked) != 0 {
		t.Fatal("a cut subject holds nothing")
	}
}

func TestLiveReleaseForgets(t *testing.T) {
	var l Live
	h := l.hold(t.Context(), "alice", Principal{Owner: "o"})
	if p, _ := FromContext(h); p.Owner != "o" {
		t.Fatalf("a held context carries its principal: %+v", p)
	}
	h.Release()
	if l.Held("alice") != 0 || h.Err() == nil {
		t.Fatalf("released: held %d, ctx %v", l.Held("alice"), h.Err())
	}
}
