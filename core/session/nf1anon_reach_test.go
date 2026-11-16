// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay/client/rpcclient"
)

// TestNF1AnonNoOtherRoute (nfltr docs/decisions/nf1-anon-2026-11-16.md
// section 2, spec/NF1Anon.tla mutant keyed-route): an agent with no
// account is reached by no keyed route, listing or A2A call, and reaches
// nothing, whoever asks, in the DEV ONLY single tenant too: only an nf1
// open (which asks no Reach) reaches it.
func TestNF1AnonNoOtherRoute(t *testing.T) {
	d := newTwoAccountDispatcher(t, AccountIsOneGroup{})
	anon := nf1anon.AgentID([16]byte{1})
	d.RegisterAgent(anon, "", "")
	anonP := identity.Principal{Provider: nf1anon.Provider, Subject: anon[len("nf1:"):], AgentID: anon}
	alice := machinePrincipal("alice", "alice@example.com")
	for _, single := range []bool{false, true} {
		d.SingleTenant = single
		for _, a := range []Action{Dial, List} {
			if ok, _ := d.Reach(alice, anon, a); ok {
				t.Errorf("single tenant %v: an account's principal reaches the anonymous agent (%s)", single, a)
			}
			if ok, _ := d.Reach(anonP, aliceNode, a); ok {
				t.Errorf("single tenant %v: the anonymous agent reaches an account's (%s)", single, a)
			}
		}
		if ok, _ := d.DialReaches(alice)(anon); ok {
			t.Errorf("single tenant %v: dialled by a keyed route", single)
		}
		for _, st := range [][2]string{{aliceHub, anon}, {anon, aliceHub}, {anon, rpcclient.RelayAgentID}} {
			if mustReach(t, d.A2AReaches, st[0], st[1]) {
				t.Errorf("single tenant %v: A2AReaches(%s, %s)", single, st[0], st[1])
			}
		}
	}
}
