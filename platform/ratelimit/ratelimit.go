// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package ratelimit provides per-key token-bucket rate limiting.
//
// It is designed for per-AgentID request throttling — each key (typically an
// agent identity or a source address) gets its own token bucket
// (golang.org/x/time/rate) that refills at a configurable rate. The keys
// live in a bounded LRU (hashicorp/golang-lru/v2): idle ones are swept
// after a TTL and, past the cap, the least recently used one is evicted and
// counted (proxy_ratelimit_evictions_total), so callers with ever new keys
// cannot grow a limiter without bound (audit F-26).
package ratelimit

import (
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"github.com/nfltr/relay-core/platform/metrics"
	"golang.org/x/time/rate"
)

// DefaultMaxKeys is the keys a limiter holds unless WithMaxKeys says
// otherwise: about 10 MB of buckets.
const DefaultMaxKeys = 100_000

// Limiter rate-limits operations on a per-key basis using the token bucket
// algorithm. Each key maintains an independent bucket.
type Limiter struct {
	mu      sync.Mutex
	buckets *simplelru.LRU[string, *bucket] // most recently used last
	rate    rate.Limit
	burst   int           // max tokens (bucket capacity)
	ttl     time.Duration // idle bucket expiry
	nowFn   func() time.Time
	maxKeys int
	// cleanupEvery is how often a call sweeps idle buckets (0 never);
	// lastCleanup is when one last did. The sweep runs on the calls that
	// add buckets, so an unused limiter runs nothing.
	cleanupEvery time.Duration
	lastCleanup  time.Time
}

type bucket struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithNowFunc overrides the time source (useful for testing).
func WithNowFunc(fn func() time.Time) Option {
	return func(l *Limiter) { l.nowFn = fn }
}

// WithMaxKeys sets the most keys the limiter holds (DefaultMaxKeys).
func WithMaxKeys(n int) Option {
	return func(l *Limiter) { l.maxKeys = n }
}

// New creates a per-key rate limiter.
//
//   - r: tokens added per second per key.
//   - burst: maximum token capacity (also the initial allowance).
//   - cleanupInterval: how often to sweep idle buckets (lazily, by the calls
//     that touch the limiter; 0 never).
//   - ttl: remove a bucket if it has been idle longer than this.
func New(r float64, burst int, cleanupInterval, ttl time.Duration, opts ...Option) *Limiter {
	l := &Limiter{
		rate:    rate.Limit(r),
		burst:   burst,
		ttl:     ttl,
		nowFn:   time.Now,
		maxKeys: DefaultMaxKeys,
	}
	for _, o := range opts {
		o(l)
	}
	l.buckets, _ = simplelru.NewLRU[string, *bucket](max(l.maxKeys, 1), nil) // errors only on a size < 1
	l.cleanupEvery, l.lastCleanup = cleanupInterval, l.nowFn()
	return l
}

// Allow reports whether the operation identified by key is allowed under the
// rate limit. If allowed, one token is consumed; otherwise the call is
// rejected without consuming a token.
func (l *Limiter) Allow(key string) bool {
	return l.AllowN(key, 1)
}

// AllowN reports whether n tokens can be consumed for the given key.
func (l *Limiter) AllowN(key string, n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.nowFn()
	return l.bucketLocked(key, now).AllowN(now, n)
}

// Blocked reports whether key has no whole token left, without consuming
// one. It lets a caller refuse work up front and spend tokens only on the
// outcomes it limits, such as failed lookups.
func (l *Limiter) Blocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.nowFn()
	return l.bucketLocked(key, now).TokensAt(now) < 1
}

// bucketLocked returns key's token bucket, made full when key has none.
// l.mu must be held.
func (l *Limiter) bucketLocked(key string, now time.Time) *rate.Limiter {
	if l.cleanupEvery > 0 && now.Sub(l.lastCleanup) >= l.cleanupEvery {
		l.cleanupLocked(now)
		l.lastCleanup = now
	}
	b, ok := l.buckets.Get(key)
	if !ok {
		b = &bucket{lim: rate.NewLimiter(l.rate, l.burst)}
		if l.buckets.Add(key, b) {
			metrics.IncrRateLimitEvictions()
		}
	}
	b.lastSeen = now
	return b.lim
}

// Len returns the number of tracked keys (for metrics / testing).
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buckets.Len()
}

// cleanupLocked removes the buckets idle past the TTL: the least recently
// used first, so it stops at the first one still in use.
func (l *Limiter) cleanupLocked(now time.Time) {
	for {
		key, b, ok := l.buckets.GetOldest()
		if !ok || now.Sub(b.lastSeen) <= l.ttl {
			return
		}
		l.buckets.Remove(key)
	}
}
