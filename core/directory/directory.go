// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package directory is the replica directory: which replica holds each
// agent's session, and the records replicas share. One replica keeps it in
// this process (MemoryCache); several share a store implementing Cache
// (Redis, SQL), which the binary that composes them provides.
package directory

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is a Get miss reported as an error (the Redis directory's;
// the in-process and SQL ones return nil, nil). Callers treat both as a miss.
var ErrNotFound = errors.New("directory: not found")

// Pong is the standard response from a cache health check.
const Pong = "PONG"

// Value is one record of the directory: the replica (PodIP) holding an
// agent's session and what it declared.
type Value struct {
	PodIP        string            `json:"PodIP"`
	Env          string            `json:"Env"`
	Labels       map[string]string `json:"Labels,omitempty"`
	Capabilities []string          `json:"Capabilities,omitempty"`
	// The agent session the record names (inventory.Session): when it
	// started (unix nanoseconds), its place in the agent's session order
	// and a tag of its credential (json "Owner", the key every relay
	// version reads). Empty in a record an older relay wrote.
	Since      int64  `json:"Since,omitempty"`
	Seq        int64  `json:"Seq,omitempty"`
	Credential string `json:"Owner,omitempty"`
	// Account is the account owning the agent (its principal's owner), so
	// a replica other than the agent's knows it; empty: no principal, or
	// written by a relay before this field.
	Account string `json:"Account,omitempty"`
	// The services the agent declared (agentservices.Format), so a replica
	// other than the agent's resolves them; empty: none, or written by a
	// relay before v0.20.3.
	Services string `json:"Services,omitempty"`
}

// Cache is the directory's store: records by key with an expiry, counters,
// and sets (the fleet label index). MemoryCache keeps it in this process.
type Cache interface {
	Ping(ctx context.Context) (string, error)
	Get(ctx context.Context, key string) (*Value, error)
	Set(ctx context.Context, key string, value any, expiration time.Duration) (string, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	// Incr adds by to the counter at key (0 when absent) and returns the
	// sum; the key then expires after ttl.
	Incr(ctx context.Context, key string, by int64, ttl time.Duration) (int64, error)

	// Redis SET operations for fleet label indexing.
	SAdd(ctx context.Context, key string, members ...string) (int64, error)
	SRem(ctx context.Context, key string, members ...string) (int64, error)
	SInter(ctx context.Context, keys ...string) ([]string, error)
}
