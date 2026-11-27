// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"
)

// relayIDKey holds the directory's relay id (RelayID).
const relayIDKey = "nfltr:relay-id"

// SetNXCache is a Cache that writes a key only when it holds none (Redis
// SET NX), reporting whether it wrote.
type SetNXCache interface {
	SetNX(ctx context.Context, key, value string, expiration time.Duration) (bool, error)
}

// RelayID returns the relay id of the directory c (Welcome.relay_id): one
// value per directory, random when the first replica asks, so every
// replica sharing c reads the same id and an independent relay its own
// (spec/Failover.tla). It fails when c cannot write a key only when it is
// absent (SetNXCache): replicas starting at once would each write theirs.
// ponytail: a store that loses its records (Directory.tla StoreLoss) loses
// the id, and a replica starting after that reads a new one; re-assert the
// id from a running replica if store loss must not split a relay's id.
func RelayID(ctx context.Context, c Cache) (string, error) {
	nx, ok := c.(SetNXCache)
	if !ok {
		return "", errors.New("directory: the store cannot write a relay id once (no SetNX)")
	}
	if _, err := nx.SetNX(ctx, relayIDKey, rand.Text(), 0); err != nil {
		return "", fmt.Errorf("directory: relay id: %w", err)
	}
	v, err := c.Get(ctx, relayIDKey)
	if err != nil {
		return "", fmt.Errorf("directory: relay id: %w", err)
	}
	if v == nil || v.PodIP == "" {
		return "", errors.New("directory: relay id written but not read back")
	}
	return v.PodIP, nil
}

// SetNX implements SetNXCache.
func (m *MemoryCache) SetNX(_ context.Context, key, value string, expiration time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.writeLocked()
	if _, ok := m.liveLocked(key); ok {
		return false, nil
	}
	m.kv[key] = memoryEntry{v: Value{PodIP: value}, exp: expiry(now, expiration)}
	return true, nil
}
