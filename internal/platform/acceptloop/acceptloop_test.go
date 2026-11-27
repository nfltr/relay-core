// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package acceptloop

import (
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestRetry(t *testing.T) {
	var b Backoff
	if b.Retry(nil, "t", fmt.Errorf("accept: %w", net.ErrClosed)) {
		t.Fatal("retried a closed listener")
	}
	before := testutil.ToFloat64(acceptErrors.WithLabelValues("t"))
	for _, want := range []time.Duration{5 * time.Millisecond, 10 * time.Millisecond} {
		if !b.Retry(nil, "t", syscall.EMFILE) || b.d != want {
			t.Fatalf("backoff %v, want %v", b.d, want)
		}
	}
	if got := testutil.ToFloat64(acceptErrors.WithLabelValues("t")) - before; got != 2 {
		t.Fatalf("counted %v errors, want 2", got)
	}
	b.d = 800 * time.Millisecond
	b.Retry(nil, "t", errors.New("other"))
	if b.d != time.Second {
		t.Fatalf("backoff %v past its cap", b.d)
	}
	b.Reset()
	if b.d != 0 {
		t.Fatal("Reset kept the backoff")
	}
}

// TestRetryDoesNotSpin: a listener that fails every Accept (a process at
// its descriptor limit) is retried a handful of times in 300 ms, not in a
// busy loop (iroh-relay issue 4636); every accept loop of the relay core
// answers its errors with Retry.
func TestRetryDoesNotSpin(t *testing.T) {
	var b Backoff
	calls := 0
	for end := time.Now().Add(300 * time.Millisecond); time.Now().Before(end); calls++ {
		b.Retry(nil, "t", syscall.EMFILE)
	}
	// 5+10+20+40+80+160 ms: 6 calls; a spin makes millions.
	if calls > 10 {
		t.Fatalf("%d retries in 300 ms of EMFILE: Retry spins", calls)
	}
}
