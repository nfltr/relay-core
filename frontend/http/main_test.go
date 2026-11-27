// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"os"
	"testing"

	"github.com/nfltr/relay-core/internal/platform/agentcliguard"
	"github.com/nfltr/relay-core/internal/platform/leakcheck"
)

// TestMain keeps these tests off every real relay and agent CLI the host or
// a node agent's environment points at.
func TestMain(m *testing.M) {
	// txnregistry's reaper goroutine stays until the registry holds no entry
	// and no tombstone (tombstones live 30 s), so a test's registry outlives
	// it by design. Follow-up: a Close on the registry the tests can call.
	os.Exit(leakcheck.RunFunc(func() int { return agentcliguard.Run(m) }, "txnregistry.(*TransactionRegistry[...]).reap"))
}
