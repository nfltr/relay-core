// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"bytes"
	"context"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
)

// reapers counts the running transaction-registry reapers.
func reapers() int {
	var buf bytes.Buffer
	_ = pprof.Lookup("goroutine").WriteTo(&buf, 2)
	return strings.Count(buf.String(), "txnregistry.(*TransactionRegistry[...]).reap(")
}

// TestIngressReaperEndsWithTheRelay: the proxy's transaction reaper, started
// by a request, ends when the relay's run (Attach's ctx) ends; it ran on
// until an idle tick (30 s) and failed relaytest's leak check.
func TestIngressReaperEndsWithTheRelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ing := New(ctx, &testHost{core: &session.Core{}})
	before := reapers()
	_, cleanup := ing.proxy.txnRegistry.Register("txn-lifetime", context.Background(), testLogger())
	defer cleanup()
	waitReapers(t, before+1, "a Register started none")
	cancel()
	waitReapers(t, before, "the reaper still runs after the relay's run ended")
}

func waitReapers(t *testing.T, want int, msg string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); reapers() != want; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d reapers, want %d", msg, reapers(), want)
		}
	}
}
