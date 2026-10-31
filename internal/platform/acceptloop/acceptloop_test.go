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
