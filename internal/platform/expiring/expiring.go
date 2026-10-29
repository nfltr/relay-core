// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package expiring is the relay's one bounded cache: a golang-lru cache
// whose entries expire lazily, when a read finds them past their deadline.
// It starts no goroutine and no ticker, so an idle relay's caches cost no
// wakeups (golang-lru's expirable cache runs a janitor goroutine per
// instance, waking every TTL/100, that never exits). An expired entry
// nobody reads again stays until the size bound evicts it.
package expiring

import (
	"math"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// Cache holds at most size entries, least recently used evicted first.
// It is safe for concurrent use.
type Cache[K comparable, V any] struct {
	l   *lru.Cache[K, entry[V]]
	ttl time.Duration
	now func() time.Time
}

type entry[V any] struct {
	v     V
	until time.Time // zero: never expires
}

// New returns a cache of at most size entries (0 or less: no bound), each
// kept ttl after it is added (0: until evicted or removed). onEvict, if not
// nil, is called for every entry that leaves the cache: evicted, expired,
// removed or purged.
func New[K comparable, V any](size int, ttl time.Duration, onEvict func(K, V)) *Cache[K, V] {
	if size <= 0 {
		size = math.MaxInt // golang-lru allocates by use, not by size
	}
	var cb func(K, entry[V])
	if onEvict != nil {
		cb = func(k K, e entry[V]) { onEvict(k, e.v) }
	}
	l, err := lru.NewWithEvict[K, entry[V]](size, cb)
	if err != nil {
		panic("expiring: " + err.Error()) // unreachable: size is positive
	}
	return &Cache[K, V]{l: l, ttl: ttl, now: time.Now}
}

// Get returns key's value unless it is absent or expired; an expired one
// is removed.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	e, ok := c.l.Get(key)
	if ok && !e.until.IsZero() && !c.now().Before(e.until) {
		c.l.Remove(key)
		ok = false
	}
	if !ok {
		var zero V
		return zero, false
	}
	return e.v, true
}

// Add stores value under key for the cache's TTL.
func (c *Cache[K, V]) Add(key K, value V) {
	var until time.Time
	if c.ttl > 0 {
		until = c.now().Add(c.ttl)
	}
	c.l.Add(key, entry[V]{v: value, until: until})
}

// AddUntil stores value under key until the given time (zero: the cache's
// TTL), or the cache's TTL from now if that is sooner.
func (c *Cache[K, V]) AddUntil(key K, value V, until time.Time) {
	if c.ttl > 0 {
		if limit := c.now().Add(c.ttl); until.IsZero() || limit.Before(until) {
			until = limit
		}
	}
	c.l.Add(key, entry[V]{v: value, until: until})
}

// Remove drops key.
func (c *Cache[K, V]) Remove(key K) { c.l.Remove(key) }

// RemoveIf drops every entry f selects and returns how many: O(entries),
// for rare invalidations (a revoked credential's answers).
func (c *Cache[K, V]) RemoveIf(f func(K, V) bool) int {
	n := 0
	for _, k := range c.l.Keys() {
		if e, ok := c.l.Peek(k); ok && f(k, e.v) {
			c.l.Remove(k)
			n++
		}
	}
	return n
}

// Purge drops every entry.
func (c *Cache[K, V]) Purge() { c.l.Purge() }

// Len returns how many entries the cache holds, expired ones included.
func (c *Cache[K, V]) Len() int { return c.l.Len() }
