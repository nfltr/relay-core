// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// MemoryCache is the directory of a single replica (cache.type=inmemory):
// the RedisCache's semantics on maps in this process, no server, no
// connection, no goroutine. Expired keys are dropped when read and, so
// keys nobody reads again (an agent's session counter) do not pile up, by
// one sweep of the whole map at most every memorySweepEvery, run by a
// write: an idle relay does nothing.
type MemoryCache struct {
	mu        sync.Mutex
	kv        map[string]memoryEntry
	sets      map[string]map[string]struct{}
	nextSweep time.Time
	now       func() time.Time // tests move the clock
}

// memoryEntry is one key: a record (Value) or a counter (Incr), as Redis
// keeps either in one key space.
type memoryEntry struct {
	v       Value
	n       int64
	counter bool
	exp     time.Time // zero: no expiry
}

const memorySweepEvery = time.Minute

// NewMemoryCache returns an empty directory.
func NewMemoryCache() *MemoryCache { return NewMemoryCacheClock(time.Now) }

// NewMemoryCacheClock returns an empty directory that reads the time from
// now (a test moves it).
func NewMemoryCacheClock(now func() time.Time) *MemoryCache {
	return &MemoryCache{kv: map[string]memoryEntry{}, sets: map[string]map[string]struct{}{}, now: now}
}

// Ping always answers Pong.
func (m *MemoryCache) Ping(context.Context) (string, error) { return Pong, nil }

// Set stores value under key; expiration 0 keeps it until deleted. value is
// a Value or, as a Redis string, its JSON or a bare pod address.
func (m *MemoryCache) Set(_ context.Context, key string, value any, expiration time.Duration) (string, error) {
	v, err := toValue(value)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.writeLocked()
	m.kv[key] = memoryEntry{v: v, exp: expiry(now, expiration)}
	return "OK", nil
}

// Get returns the record at key, or nil, nil when there is none (the SQL
// cache's miss; the Redis one returns ErrNotFound, which callers handle as
// the same).
func (m *MemoryCache) Get(_ context.Context, key string) (*Value, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.liveLocked(key)
	if !ok {
		return nil, nil
	}
	if e.counter {
		return &Value{PodIP: fmt.Sprint(e.n)}, nil
	}
	v := e.v
	v.Labels = maps.Clone(v.Labels)
	v.Capabilities = slices.Clone(v.Capabilities)
	return &v, nil
}

// Del removes keys and returns how many existed.
func (m *MemoryCache) Del(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int64
	for _, k := range keys {
		if _, ok := m.liveLocked(k); ok {
			delete(m.kv, k)
			n++
		} else if _, ok := m.sets[k]; ok {
			delete(m.sets, k)
			n++
		}
	}
	return n, nil
}

// Incr implements Cache.Incr.
func (m *MemoryCache) Incr(_ context.Context, key string, by int64, ttl time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.writeLocked()
	e, ok := m.liveLocked(key)
	if ok && !e.counter {
		return 0, fmt.Errorf("incr %s: value is not an integer", key)
	}
	e = memoryEntry{n: e.n + by, counter: true, exp: expiry(now, ttl)}
	m.kv[key] = e
	return e.n, nil
}

// SAdd adds members to the set at key and returns how many were new.
func (m *MemoryCache) SAdd(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sets[key]
	if s == nil {
		s = map[string]struct{}{}
		m.sets[key] = s
	}
	var n int64
	for _, x := range members {
		if _, ok := s[x]; !ok {
			s[x] = struct{}{}
			n++
		}
	}
	return n, nil
}

// SRem removes members from the set at key and returns how many were in it.
func (m *MemoryCache) SRem(_ context.Context, key string, members ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sets[key]
	var n int64
	for _, x := range members {
		if _, ok := s[x]; ok {
			delete(s, x)
			n++
		}
	}
	if len(s) == 0 {
		delete(m.sets, key) // Redis drops an empty set
	}
	return n, nil
}

// SInter returns the members every set at keys holds.
func (m *MemoryCache) SInter(_ context.Context, keys ...string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []string{}
	if len(keys) == 0 {
		return out, nil
	}
next:
	for x := range m.sets[keys[0]] {
		for _, k := range keys[1:] {
			if _, ok := m.sets[k][x]; !ok {
				continue next
			}
		}
		out = append(out, x)
	}
	return out, nil
}

// liveLocked returns key's entry, dropping it when it expired.
func (m *MemoryCache) liveLocked(key string) (memoryEntry, bool) {
	e, ok := m.kv[key]
	if ok && !e.exp.IsZero() && !m.now().Before(e.exp) {
		delete(m.kv, key)
		return memoryEntry{}, false
	}
	return e, ok
}

// writeLocked returns the time and sweeps expired keys when the last sweep
// is memorySweepEvery old.
// ponytail: O(keys) under the lock once a minute; per-bucket expiry if a
// replica ever holds enough keys for that pause to matter.
func (m *MemoryCache) writeLocked() time.Time {
	now := m.now()
	if now.Before(m.nextSweep) {
		return now
	}
	m.nextSweep = now.Add(memorySweepEvery)
	for k, e := range m.kv {
		if !e.exp.IsZero() && !now.Before(e.exp) {
			delete(m.kv, k)
		}
	}
	return now
}

func expiry(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

// toValue is what RedisCache.Get would read back after RedisCache.Set.
func toValue(value any) (Value, error) {
	var raw string
	switch v := value.(type) {
	case Value:
		v.Labels = maps.Clone(v.Labels)
		v.Capabilities = slices.Clone(v.Capabilities)
		return v, nil
	case string:
		raw = v
	case []byte:
		raw = string(v)
	default:
		raw = fmt.Sprint(v)
	}
	var v Value
	if json.Unmarshal([]byte(raw), &v) != nil {
		return Value{PodIP: raw}, nil
	}
	return v, nil
}
