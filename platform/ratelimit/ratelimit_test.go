// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package ratelimit

import (
	"testing"
	"time"
)

func TestAllow_UnderLimit(t *testing.T) {
	now := time.Now()
	l := New(10, 10, 0, time.Minute, WithNowFunc(func() time.Time { return now }))

	for i := range 10 {
		if !l.Allow("a") {
			t.Fatalf("request %d should be allowed", i)
		}
	}
	// 11th request should be rejected (burst=10, no time passed).
	if l.Allow("a") {
		t.Fatal("request 11 should be rejected")
	}
}

func TestAllow_Refill(t *testing.T) {
	now := time.Now()
	l := New(10, 10, 0, time.Minute, WithNowFunc(func() time.Time { return now }))

	// Drain all tokens.
	for range 10 {
		l.Allow("a")
	}
	if l.Allow("a") {
		t.Fatal("should be rejected when drained")
	}

	// Advance 1 second -> 10 new tokens.
	now = now.Add(time.Second)
	for i := range 10 {
		if !l.Allow("a") {
			t.Fatalf("request %d after refill should be allowed", i)
		}
	}
}

func TestAllow_PerKey(t *testing.T) {
	now := time.Now()
	l := New(1, 1, 0, time.Minute, WithNowFunc(func() time.Time { return now }))

	if !l.Allow("a") {
		t.Fatal("key a first request should be allowed")
	}
	if l.Allow("a") {
		t.Fatal("key a second request should be rejected")
	}
	// Different key should have its own bucket.
	if !l.Allow("b") {
		t.Fatal("key b first request should be allowed")
	}
}

func TestAllowN(t *testing.T) {
	now := time.Now()
	l := New(10, 10, 0, time.Minute, WithNowFunc(func() time.Time { return now }))

	if !l.AllowN("a", 5) {
		t.Fatal("5 tokens should be available")
	}
	if !l.AllowN("a", 5) {
		t.Fatal("5 more tokens should be available")
	}
	if l.AllowN("a", 1) {
		t.Fatal("no tokens left")
	}
}

// The sweep runs lazily, on the first call a cleanup interval after the
// last one: no goroutine, so an unused limiter never wakes.
func TestCleanup(t *testing.T) {
	now := time.Now()
	l := New(10, 10, time.Minute, time.Minute, WithNowFunc(func() time.Time { return now }))

	l.Allow("a")
	l.Allow("b")
	if l.Len() != 2 {
		t.Fatalf("expected 2 keys, got %d", l.Len())
	}

	// Advance past TTL for key "a" only by touching "b" mid-way.
	now = now.Add(30 * time.Second)
	l.Allow("b") // refresh "b"
	now = now.Add(31 * time.Second)
	l.Allow("b") // a minute after New: sweeps "a"

	if l.Len() != 1 {
		t.Fatalf("expected 1 key after cleanup, got %d", l.Len())
	}
	// "b" should still be present.
	if !l.Allow("b") {
		t.Fatal("key b should still exist")
	}
}

func TestBurstCap(t *testing.T) {
	now := time.Now()
	l := New(100, 5, 0, time.Minute, WithNowFunc(func() time.Time { return now }))

	// Drain tokens.
	for range 5 {
		l.Allow("a")
	}

	// Advance 10 seconds -> would add 1000 tokens, but burst caps at 5.
	now = now.Add(10 * time.Second)
	for i := range 5 {
		if !l.Allow("a") {
			t.Fatalf("request %d should be allowed (burst refill cap)", i)
		}
	}
	if l.Allow("a") {
		t.Fatal("6th request should be rejected (burst cap)")
	}
}

func TestLen(t *testing.T) {
	l := New(10, 10, 0, time.Minute)
	if l.Len() != 0 {
		t.Fatalf("expected 0, got %d", l.Len())
	}
	l.Allow("x")
	l.Allow("y")
	l.Allow("z")
	if l.Len() != 3 {
		t.Fatalf("expected 3, got %d", l.Len())
	}
}

func TestZeroRate(t *testing.T) {
	now := time.Now()
	l := New(0, 1, 0, time.Minute, WithNowFunc(func() time.Time { return now }))

	if !l.Allow("a") {
		t.Fatal("first request should succeed (initial burst)")
	}
	now = now.Add(time.Hour)
	if l.Allow("a") {
		t.Fatal("should never refill with rate=0")
	}
}

func BenchmarkAllow(b *testing.B) {
	l := New(1000, 1000, 0, time.Minute)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow("bench-key")
	}
}

func BenchmarkAllow_ManyKeys(b *testing.B) {
	l := New(1000, 1000, 0, time.Minute)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "key-" + string(rune('A'+i%26)) + string(rune('0'+i%10))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.Allow(keys[i%len(keys)])
	}
}

func TestBlocked_DoesNotConsume(t *testing.T) {
	now := time.Now()
	l := New(1, 1, 0, time.Minute, WithNowFunc(func() time.Time { return now }))
	for range 3 {
		if l.Blocked("k") {
			t.Fatal("Blocked consumed the only token")
		}
	}
	if !l.Allow("k") || !l.Blocked("k") {
		t.Fatal("want blocked once the token is spent")
	}
	now = now.Add(time.Second)
	if l.Blocked("k") {
		t.Fatal("want unblocked after refill")
	}
}
