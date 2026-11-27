// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package txnregistry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A transaction whose consumer takes nothing must not hold the Fanout
// caller (an agent's receive loop, a broker fanout worker) past Stall: the
// transaction is refused once (Stalled closes), its later responses are
// stale at once, and the caller's other transactions go on. Before, Fanout
// waited until the stuck transaction's context ended.
func TestFanoutRefusesAStuckTransaction(t *testing.T) {
	r := NewTransactionRegistry[int]()
	r.Stall = 20 * time.Millisecond
	logger := newTestLogger()
	stuck, cleanStuck := r.Register("stuck", context.Background(), logger)
	defer cleanStuck()
	other, cleanOther := r.Register("other", context.Background(), logger)
	defer cleanOther()

	var stalled, stale int
	loop := make(chan struct{})
	go func() {
		defer close(loop)
		for i := range 40 {
			switch err := r.Fanout("stuck", i, logger); {
			case errors.Is(err, ErrTransactionStalled):
				stalled++
			case errors.Is(err, ErrTransactionStale):
				stale++
			}
		}
		_ = r.Fanout("other", 1, logger)
	}()
	select {
	case <-loop:
	case <-time.After(5 * time.Second):
		t.Fatal("a transaction whose consumer takes nothing holds the Fanout caller")
	}
	if stalled != 1 || stale != 40-16-1 {
		t.Fatalf("stalled %d, stale %d; want 1 refusal, then every later response stale", stalled, stale)
	}
	select {
	case <-stuck.Stalled:
	default:
		t.Fatal("the stuck transaction's consumer was not told it was refused")
	}
	select {
	case v := <-other.ResponseCh:
		if v != 1 {
			t.Fatalf("other got %d", v)
		}
	default:
		t.Fatal("the caller's other transaction got nothing")
	}
	select {
	case <-other.Stalled:
		t.Fatal("a consumer that took was refused")
	default:
	}
}
