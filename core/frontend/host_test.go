// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
	"testing"

	"github.com/nfltr/relay-core/core/session"
)

// TestCoreHostVisibleIsTheKeysOwnAgents: the core's WireGuard exits are
// the key base's own agents, the rule its sessions reach by.
func TestCoreHostVisibleIsTheKeysOwnAgents(t *testing.T) {
	visible := (&CoreHost{C: &session.Core{Dispatcher: &session.Dispatcher{}}}).Visible("alice")
	for id, want := range map[string]bool{"alice": true, "alice.exit": true, "bob.exit": false, "aliceexit": false} {
		if got := visible(id); got != want {
			t.Errorf("Visible(alice)(%q) = %v, want %v", id, got, want)
		}
	}
}
