// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/util"
)

// TestCountsAgainstCap (spec/NF1Cap.tla Counted, NoCapOnOwner, mutant
// count-owner): a session counts against its address's cap unless its
// client is the agent owner's own account; an owner not known counts; an
// anonymous agent's never (nf1anon budgets it).
func TestCountsAgainstCap(t *testing.T) {
	owners := &util.SyncMap[string, string]{}
	owners.Store("srv", "acct-a")
	f := &Frontend{Core: &session.Core{Dispatcher: &session.Dispatcher{AgentOwners: owners}}}
	for _, c := range []struct {
		caller, agent string
		want          bool
	}{
		{"", "srv", true},       // no credential
		{"acct-b", "srv", true}, // another account
		{"acct-a", "srv", false},
		{"acct-a", "unknown", true},
		{"", "nf1:00", false},
	} {
		if got := f.countsAgainstCap(identity.Principal{Owner: c.caller}, c.agent); got != c.want {
			t.Errorf("caller %q to %q: counted %v, want %v", c.caller, c.agent, got, c.want)
		}
	}
}
