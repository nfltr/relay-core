// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package ratelimit provides per-key token-bucket rate limiting.
//
// It is designed for per-AgentID request throttling — each key (typically an
// agent identity) gets its own token bucket that refills at a configurable
// rate. Idle buckets are automatically cleaned up to prevent unbounded memory
// growth.
package ratelimit

import (
	"sync"
	"time"
)

// Limiter rate-limits operations on a per-key basis using the token bucket
// algorithm. Each key maintains an independent bucket.
type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	rate    float64       // tokens added per second
	burst   int           // max tokens (bucket capacity)
	ttl     time.Duration // idle bucket expiry
	nowFn   func() time.Time
	// cleanupEvery is how often a call sweeps idle buckets (0 never);
	// lastCleanup is when one last did. The sweep runs on the calls that
	// add buckets, so an unused limiter runs nothing.
	cleanupEvery time.Duration
	lastCleanup  time.Time
}

type bucket struct {
	tokens   float64
	lastSeen time.Time
}

// Option configures a Limiter.
type Option func(*Limiter)

// WithNowFunc overrides the time source (useful for testing).
func WithNowFunc(fn func() time.Time) Option {
	return func(l *Limiter) { l.nowFn = fn }
}

// New creates a per-key rate limiter.
//
//   - rate: tokens added per second per key.
//   - burst: maximum token capacity (also the initial allowance).
//   - cleanupInterval: how often to sweep idle buckets (lazily, by the calls
//     that touch the limiter; 0 never).
//   - ttl: remove a bucket if it has been idle longer than this.
func New(rate float64, burst int, cleanupInterval, ttl time.Duration, opts ...Option) *Limiter {
	l := &Limiter{
		buckets: make(map[string]*bucket),
		rate:    rate,
		burst:   burst,
		ttl:     ttl,
		nowFn:   time.Now,
	}
	for _, o := range opts {
		o(l)
	}
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
	b := l.refillLocked(key)
	if b.tokens < float64(n) {
		return false
	}
	b.tokens -= float64(n)
	return true
}

// Blocked reports whether key has no whole token left, without consuming
// one. It lets a caller refuse work up front and spend tokens only on the
// outcomes it limits, such as failed lookups.
func (l *Limiter) Blocked(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refillLocked(key).tokens < 1
}

// refillLocked returns key's bucket, refilled for the time since it was last
// seen. l.mu must be held.
func (l *Limiter) refillLocked(key string) *bucket {
	now := l.nowFn()
	if l.cleanupEvery > 0 && now.Sub(l.lastCleanup) >= l.cleanupEvery {
		l.cleanupLocked(now)
		l.lastCleanup = now
	}
	b, ok := l.buckets[key]
	if !ok {
		b = &bucket{tokens: float64(l.burst), lastSeen: now}
		l.buckets[key] = b
	}
	b.tokens = min(b.tokens+now.Sub(b.lastSeen).Seconds()*l.rate, float64(l.burst))
	b.lastSeen = now
	return b
}

// Len returns the number of tracked keys (for metrics / testing).
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

func (l *Limiter) cleanupLocked(now time.Time) {
	for key, b := range l.buckets {
		if now.Sub(b.lastSeen) > l.ttl {
			delete(l.buckets, key)
		}
	}
}
