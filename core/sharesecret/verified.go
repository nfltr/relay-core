// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharesecret

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// VerifiedCache answers a share credential that already matched its sealed
// secret without another argon2 evaluation, so a browser's burst of requests
// with the same Basic Auth (or a client reusing its token) costs one hash,
// not one per request against the relay-wide hash limit.
//
// An entry is keyed on an HMAC (a per-process random key) of the share code,
// the sealed value and the presented credential; no plaintext is kept, and a
// digest is useless outside this process. Only matches are cached. Keying on
// the sealed value means a reissued share (fresh salt) never hits an old
// entry. Entries are bounded by count and TTL, end no later than the share's
// ExpiresAt, and Forget drops a code's entries on revoke. The request path
// validates the share code before any credential check, so an entry of a
// revoked or expired share is never consulted even before it is dropped.
// Concurrent cold checks of one credential share a single evaluation; cold
// checks still go through SecretMatches' fail-closed hash limit.
type VerifiedCache struct {
	key    [32]byte
	max    int
	ttl    time.Duration
	now    func() time.Time
	flight singleflight.Group

	mu      sync.Mutex
	entries map[[sha256.Size]byte]verifiedEntry
}

type verifiedEntry struct {
	code  string
	until time.Time
}

// NewVerifiedCache returns a cache of at most max entries, each kept ttl.
func NewVerifiedCache(max int, ttl time.Duration) *VerifiedCache {
	c := &VerifiedCache{max: max, ttl: ttl, now: time.Now, entries: map[[sha256.Size]byte]verifiedEntry{}}
	if _, err := rand.Read(c.key[:]); err != nil {
		panic("sharesecret: cannot read random bytes: " + err.Error())
	}
	return c
}

// Matches is SecretMatches(sealed, presented) for share code, whose expiry
// (policy.ExpiresAt, RFC3339; "" for none known) bounds the cached answer.
func (c *VerifiedCache) Matches(code, sealed, presented, expiresAt string) bool {
	if c == nil || sealed == "" || presented == "" {
		return SecretMatches(sealed, presented) // no cache, or decided without hashing
	}
	d := c.digest(code, sealed, presented)
	now := c.now()
	c.mu.Lock()
	e, ok := c.entries[d]
	if ok && !now.Before(e.until) {
		delete(c.entries, d)
		ok = false
	}
	c.mu.Unlock()
	if ok {
		return true
	}
	v, _, _ := c.flight.Do(string(d[:]), func() (any, error) {
		return SecretMatches(sealed, presented), nil
	})
	if !v.(bool) {
		return false
	}
	until := now.Add(c.ttl)
	if exp, err := time.Parse(time.RFC3339, expiresAt); err == nil && exp.Before(until) {
		until = exp
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.entries[d]; !ok && len(c.entries) >= c.max {
		c.evictLocked(now)
	}
	c.entries[d] = verifiedEntry{code: code, until: until}
	return true
}

// Forget drops every entry of code (the share was revoked).
func (c *VerifiedCache) Forget(code string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for d, e := range c.entries {
		if e.code == code {
			delete(c.entries, d)
		}
	}
}

// evictLocked drops expired entries, then, if still full, arbitrary ones.
// ponytail: arbitrary (map order) eviction when full of live entries; an LRU
// if a relay ever holds more live verified credentials than max.
func (c *VerifiedCache) evictLocked(now time.Time) {
	for d, e := range c.entries {
		if !now.Before(e.until) {
			delete(c.entries, d)
		}
	}
	for d := range c.entries {
		if len(c.entries) < c.max {
			return
		}
		delete(c.entries, d)
	}
}

func (c *VerifiedCache) digest(fields ...string) [sha256.Size]byte {
	mac := hmac.New(sha256.New, c.key[:])
	var n [8]byte
	for _, f := range fields {
		binary.BigEndian.PutUint64(n[:], uint64(len(f)))
		mac.Write(n[:])
		mac.Write([]byte(f))
	}
	var d [sha256.Size]byte
	copy(d[:], mac.Sum(nil))
	return d
}
