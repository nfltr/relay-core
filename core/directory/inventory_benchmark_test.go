// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package directory

import (
	"testing"
)

func BenchmarkGetDispatcherCacheKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = GetDispatcherCacheKey("agentID-1234-5678-abcd")
	}
}

func BenchmarkGetStreamerCacheKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = GetStreamerCacheKey("agentID-1234-5678-abcd")
	}
}

func BenchmarkGetDispatcherCacheKey_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = GetDispatcherCacheKey("agentID-1234-5678-abcd")
		}
	})
}

func BenchmarkGetStreamerCacheKey_Parallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = GetStreamerCacheKey("agentID-1234-5678-abcd")
		}
	})
}

// --- Fleet label key benchmarks ---
