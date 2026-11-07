// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend

import (
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/util"
)

// TestCoreHostVisibleIsReach: the core's WireGuard exits are the agents the
// principal reaches (its account's, under the core's one-group policy).
func TestCoreHostVisibleIsReach(t *testing.T) {
	d := &session.Dispatcher{Policy: session.AccountIsOneGroup{}, AgentOwners: &util.SyncMap[string, string]{}}
	mine, theirs := identity.NewMachineID(), identity.NewMachineID()
	own := identity.AgentRef{Machine: mine, Service: "exit"}.String()
	other := identity.AgentRef{Machine: theirs, Service: "exit"}.String()
	d.AgentOwners.Store(own, "alice")
	d.AgentOwners.Store(other, "bob")
	visible := (&CoreHost{C: &session.Core{Dispatcher: d}}).Visible(identity.Principal{Owner: "alice", Machine: mine, Subject: "c"})
	for id, want := range map[string]bool{own: true, other: false, "alice.exit": false} {
		if got := visible(id); got != want {
			t.Errorf("Visible(alice)(%q) = %v, want %v", id, got, want)
		}
	}
}
