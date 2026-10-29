// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package expiring

import (
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestExpiryBoundsAndInvalidation(t *testing.T) {
	defer goleak.VerifyNone(t) // no janitor goroutine
	now := time.Unix(1000, 0)
	var evicted []string
	c := New[string, int](2, time.Minute, func(k string, _ int) { evicted = append(evicted, k) })
	c.now = func() time.Time { return now }

	c.Add("a", 1)
	c.AddUntil("b", 2, now.Add(time.Second)) // sooner than the TTL
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("a: %v %v", v, ok)
	}
	now = now.Add(2 * time.Second)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b outlived its deadline")
	}
	c.AddUntil("c", 3, now.Add(time.Hour)) // capped at the TTL
	now = now.Add(time.Minute)
	if _, ok := c.Get("c"); ok {
		t.Fatal("c outlived the cache's TTL")
	}
	// The size bound evicts the least recently used.
	c.Add("x", 1)
	c.Add("y", 2)
	c.Add("z", 3)
	if _, ok := c.Get("x"); ok || c.Len() != 2 {
		t.Fatalf("bound 2: x still in, len %d", c.Len())
	}
	if n := c.RemoveIf(func(k string, _ int) bool { return k == "y" }); n != 1 {
		t.Fatalf("RemoveIf removed %d", n)
	}
	c.Purge()
	if c.Len() != 0 {
		t.Fatal("purged cache not empty")
	}
	if len(evicted) < 5 {
		t.Fatalf("onEvict saw %v", evicted)
	}
}

func TestNoTTLKeepsUntilEvicted(t *testing.T) {
	defer goleak.VerifyNone(t)
	c := New[string, int](1, 0, nil)
	c.Add("a", 1)
	c.now = func() time.Time { return time.Now().Add(24 * time.Hour) }
	if _, ok := c.Get("a"); !ok {
		t.Fatal("an entry with no TTL expired")
	}
}
