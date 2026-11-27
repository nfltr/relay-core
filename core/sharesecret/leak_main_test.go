// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharesecret

import (
	"os"
	"testing"

	"github.com/nfltr/relay-core/internal/platform/leakcheck"
)

func TestMain(m *testing.M) { os.Exit(leakcheck.Run(m)) }
