// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"os"
	"testing"

	"github.com/nfltr/relay-core/internal/platform/leakcheck"
)

// TestMain fails the package when its tests leave goroutines running. Its
// tests count transfers with no session behind them: no transfer check.
func TestMain(m *testing.M) {
	leakcheck.SkipTransfers = true
	os.Exit(leakcheck.Run(m))
}
