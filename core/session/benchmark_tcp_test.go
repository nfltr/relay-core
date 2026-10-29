// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay-core/internal/sharestore"
)

// BenchmarkTCPDispatcherDialAgent measures the latency of DialAgent when the
// target agent is locally registered.  This exercises the session allocation,
// pipe creation, open-frame send, and net.Conn construction — the hot path
// for every SSH proxy and browser terminal connection.
func BenchmarkTCPDispatcherDialAgent(b *testing.B) {
	b.ReportAllocs()
	ctx := b.Context()

	d := NewTCPDispatcher(slog.Default(), nil, nil)
	agentID := "bench-agent"

	// Register a mock agent stream.
	as := NewTCPAgentStream()
	d.Mu.Lock()
	d.Streams[agentID] = as
	d.Mu.Unlock()

	// Consume sends so the channel doesn't block.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-as.SendCh:
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, err := d.DialAgent(ctx, agentID, "localhost:22")
		if err != nil {
			b.Fatal(err)
		}
		conn.Close() //nolint:errcheck
	}
}

// BenchmarkTCPDispatcherDialAgent_Parallel measures throughput of concurrent
// DialAgent calls.
func BenchmarkTCPDispatcherDialAgent_Parallel(b *testing.B) {
	b.ReportAllocs()
	ctx := b.Context()

	d := NewTCPDispatcher(slog.Default(), nil, nil)
	agentID := "bench-agent-par"

	as := NewTCPAgentStream()
	d.Mu.Lock()
	d.Streams[agentID] = as
	d.Mu.Unlock()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-as.SendCh:
			}
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := d.DialAgent(ctx, agentID, "localhost:22")
			if err != nil {
				b.Fatal(err)
			}
			conn.Close() //nolint:errcheck
		}
	})
}

// BenchmarkShareStore_Issue measures share code generation throughput.
func BenchmarkShareStore_Issue(b *testing.B) {
	b.ReportAllocs()
	store := sharestore.NewInMemory(time.Hour)
	defer store.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Issue("bench-agentID", time.Hour, sharestore.Policy{})
	}
}

// BenchmarkShareStore_Validate measures share code validation throughput.
func BenchmarkShareStore_Validate(b *testing.B) {
	b.ReportAllocs()
	store := sharestore.NewInMemory(time.Hour)
	defer store.Close()
	code := store.Issue("bench-agentID", time.Hour, sharestore.Policy{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.Validate(code)
	}
}

// BenchmarkShareStore_Validate_Parallel measures concurrent validation.
func BenchmarkShareStore_Validate_Parallel(b *testing.B) {
	b.ReportAllocs()
	store := sharestore.NewInMemory(time.Hour)
	defer store.Close()
	code := store.Issue("bench-agentID", time.Hour, sharestore.Policy{})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Validate(code)
		}
	})
}

// BenchmarkShareStore_Issue_Parallel measures concurrent issuance.
func BenchmarkShareStore_Issue_Parallel(b *testing.B) {
	b.ReportAllocs()
	store := sharestore.NewInMemory(time.Hour)
	defer store.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Issue("bench-agentID", time.Hour, sharestore.Policy{})
		}
	})
}
