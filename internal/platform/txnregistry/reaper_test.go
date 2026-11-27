// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package txnregistry

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// The reaper runs only while the registry holds something: none before the
// first Register, one after, none again once a tick leaves it empty (and the
// size gauge got a last 0).
func TestReaperRunsOnlyWhileNotEmpty(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := NewTransactionRegistry[int]()
	var ticks, last atomic.Int64
	last.Store(-1)
	r.StartReaper(ctx, 5*time.Millisecond, logger, func(size int) { ticks.Add(1); last.Store(int64(size)) })
	time.Sleep(30 * time.Millisecond)
	if r.reaper.running.Load() || ticks.Load() != 0 {
		t.Fatalf("reaper ran on an empty registry: running=%v ticks=%d", r.reaper.running.Load(), ticks.Load())
	}

	reqCtx, reqCancel := context.WithCancel(context.Background())
	_, _ = r.Register("t1", reqCtx, logger) // leaked: no cleanup call
	if !r.reaper.running.Load() {
		t.Fatal("Register did not start the reaper")
	}
	reqCancel()
	// The tombstone the reap leaves lives transactionTombstoneTTL; expire it
	// once the reap stored it (it deletes the entry first: expiring it on
	// Size() == 0 alone raced that Store, which then kept the reaper up 30 s).
	deadline := time.Now().Add(5 * time.Second)
	for !tombstoned(r, "t1") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.Size() != 0 || !tombstoned(r, "t1") {
		t.Fatal("reaper did not remove the cancelled entry")
	}
	r.tombstones.Store("t1", time.Now())
	deadline = time.Now().Add(5 * time.Second)
	for r.reaper.running.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if r.reaper.running.Load() || last.Load() != 0 {
		t.Fatalf("reaper still running on an empty registry (running=%v, last size %d)", r.reaper.running.Load(), last.Load())
	}
}

func tombstoned(r *TransactionRegistry[int], id string) bool {
	_, ok := r.tombstones.Load(id)
	return ok
}
