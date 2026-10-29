// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// Keys nobody reads again (an agent's session counter) do not pile up: a
// write sweeps expired keys once a minute. And the directory runs no
// goroutine of its own.
func TestMemoryCacheSweepsUnreadKeys(t *testing.T) {
	before := runtime.NumGoroutine()
	m := NewMemoryCache()
	if n := runtime.NumGoroutine(); n != before {
		t.Fatalf("NewMemoryCache started %d goroutines", n-before)
	}
	now := time.Unix(1_800_000_000, 0)
	m.now = func() time.Time { return now }
	ctx := context.Background()
	for _, k := range []string{"a", "b", "c"} {
		_, _ = m.Incr(ctx, k, 1, time.Second)
	}
	now = now.Add(2 * memorySweepEvery)
	_, _ = m.Set(ctx, "live", Value{PodIP: "x"}, time.Hour)
	if len(m.kv) != 1 {
		t.Fatalf("after a sweep %d keys, want 1", len(m.kv))
	}
}
