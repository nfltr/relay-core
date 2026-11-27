// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package identity

import (
	"slices"
	"testing"
)

// TestRevocationTraceClearsTheCacheBeforeTheCut: a revocation records
// "cache", then "cut" (relay-server spec/Revocation.tla ApplyCache, then
// ApplyCut), and at "cache" the key's cached answer is already gone and
// its held connection not yet cut.
func TestRevocationTraceClearsTheCacheBeforeTheCut(t *testing.T) {
	f := &fakeProvider{name: "keys", tokens: map[string]Principal{
		"k": {Owner: "o", Subject: "agent", CacheTTL: 1 << 40},
	}}
	a := NewAuthenticator("keys", f)
	c := Credential{Token: "k"}
	p, err := a.Authenticate(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	held := a.Hold(t.Context(), p)
	defer held.Release()
	var evs []string
	a.TraceRevocations(func(ev, key string) {
		if key != p.Key() {
			t.Errorf("%s of %q, want %q", ev, key, p.Key())
		}
		_, cached := a.cache.Get(keyOf(c))
		if ev == "cache" && (cached || held.Err() != nil) {
			t.Errorf("at cache: cached %v, cut %v; want the cache cleared, the connection held", cached, held.Err() != nil)
		}
		if ev == "cut" && held.Err() == nil {
			t.Error("at cut: the connection still held")
		}
		evs = append(evs, ev)
	})
	a.Revoke(p.Key())
	if want := []string{"cache", "cut"}; !slices.Equal(evs, want) {
		t.Fatalf("recorded %v, want %v", evs, want)
	}
}
