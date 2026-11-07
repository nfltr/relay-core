// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

func TestParseAgentAddress(t *testing.T) {
	ref := identity.AgentRef{Machine: testMachine("alice"), Service: "node"}
	for _, tc := range []struct {
		in   string
		want AgentAddress
		ok   bool
	}{
		{ref.String(), AgentAddress{Ref: ref, Service: "node"}, true},
		{"db", AgentAddress{Service: "db", Own: true}, true},
		{"home.db", AgentAddress{Machine: "home", Service: "db"}, true},
		{"home." + ref.String(), AgentAddress{}, false}, // a service is never a ref
		{"", AgentAddress{}, false},
		{"Home.db", AgentAddress{}, false},
		{"home.db.x", AgentAddress{}, false},
		{"home/db", AgentAddress{}, false},
		{"..", AgentAddress{}, false},
		{".db", AgentAddress{}, false},
		{"home.", AgentAddress{}, false},
		{"-db", AgentAddress{}, false},
	} {
		got, ok := ParseAgentAddress(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("ParseAgentAddress(%q) = %+v, %v; want %+v, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

// namePolicy is AccountIsOneGroup with a name table per account.
type namePolicy struct {
	AccountIsOneGroup
	names map[string]map[string]identity.MachineID // account -> name -> machine
	err   error
}

func (n *namePolicy) ResolveMachine(p identity.Principal, name string) (identity.MachineID, error) {
	return n.names[p.Owner][name], n.err
}

// TestDialResolvesNameOnce (spec Reach.tla Dial, DialOwn,
// BareNameIsOwnMachine): connect resolves the address once to the ref the
// session carries; a bare service is the caller's own machine only;
// another account's same name never resolves; a rename after the open
// changes nothing the session holds.
func TestDialResolvesNameOnce(t *testing.T) {
	names := &namePolicy{names: map[string]map[string]identity.MachineID{
		"alice@example.com": {"laptop": testMachine("alice-2"), "hub": testMachine("alice")},
		"bob@example.com":   {"laptop": testMachine("bob")},
	}}
	d := newTwoAccountDispatcher(t, names)
	d.TCPDispatcher = NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	reach := d.KeyReach(func() TargetRules { return TargetRules{} })
	alice := machinePrincipal("alice", "alice@example.com")
	d.RegisterAgent(testRef("alice", "db"), "alice@example.com", "")
	d.RegisterAgent(testRef("bob", "laptop"), "bob@example.com", "")

	for _, tc := range []struct {
		name, agent, want string
	}{
		{"a ref", aliceLaptop, aliceLaptop},
		{"a bare service: this machine's", "node", aliceNode},
		{"a machine name of the account", "laptop.laptop", aliceLaptop},
	} {
		got, _, err := reach(t.Context(), alice, Target{Agent: tc.agent, Service: "22"})
		if err != nil || got != tc.want {
			t.Errorf("%s: connect %q = %q, %v; want %q", tc.name, tc.agent, got, err, tc.want)
		}
	}
	for _, tc := range []struct {
		name, agent string
	}{
		// alice-2 serves laptop, alice does not: a bare name never reaches
		// another machine of the account.
		{"a bare service another machine serves", "laptop"},
		{"an unknown machine name", "desk.node"},
		// bob has a machine "laptop" too; alice's laptop.node is alice-2's
		// node (not registered), never bob's.
		{"another account's name", "laptop.node"},
	} {
		got, _, err := reach(t.Context(), alice, Target{Agent: tc.agent, Service: "22"})
		var r *Refusal
		if !errors.As(err, &r) || r.Reason != relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND || got != "" {
			t.Errorf("%s: connect %q = %q, %v; want AGENT_NOT_FOUND", tc.name, tc.agent, got, err)
		}
	}
	bob := machinePrincipal("bob", "bob@example.com")
	if got, _, err := reach(t.Context(), bob, Target{Agent: "laptop.laptop", Service: "22"}); err != nil || got != testRef("bob", "laptop") {
		t.Errorf("bob's laptop.laptop = %q, %v; want bob's own ref", got, err)
	}

	// The session holds the ref: renaming the machine afterwards changes
	// what the name resolves to next time, not the ref returned.
	ref, _, err := reach(t.Context(), alice, Target{Agent: "laptop.laptop", Service: "22"})
	if err != nil {
		t.Fatal(err)
	}
	names.names["alice@example.com"] = map[string]identity.MachineID{"desk": testMachine("alice-2")}
	if ref != aliceLaptop {
		t.Errorf("held ref %q, want %q", ref, aliceLaptop)
	}
	if _, _, err := reach(t.Context(), alice, Target{Agent: "laptop.laptop", Service: "22"}); !refusedNotFound(err) {
		t.Errorf("the old name after the rename: %v, want AGENT_NOT_FOUND", err)
	}
	if got, _, err := reach(t.Context(), alice, Target{Agent: "desk.laptop", Service: "22"}); err != nil || got != aliceLaptop {
		t.Errorf("the new name: %q, %v; want %q", got, err, aliceLaptop)
	}

	// The store unavailable is the relay's unavailability, not a missing
	// agent; a relay with no name table resolves no names.
	names.err = errors.New("down")
	if _, _, err := reach(t.Context(), alice, Target{Agent: "desk.laptop", Service: "22"}); !errors.As(err, new(*Refusal)) || err.(*Refusal).Reason != relayv1.ErrorReason_ERROR_REASON_DIRECTORY_UNAVAILABLE {
		t.Errorf("store down: %v, want DIRECTORY_UNAVAILABLE", err)
	}
	d.Policy = AccountIsOneGroup{}
	if _, _, err := reach(t.Context(), alice, Target{Agent: "desk.laptop", Service: "22"}); !refusedNotFound(err) {
		t.Errorf("no name table: %v, want AGENT_NOT_FOUND", err)
	}
	if got, _, err := reach(t.Context(), alice, Target{Agent: "node", Service: "22"}); err != nil || got != aliceNode {
		t.Errorf("no name table, a bare service: %q, %v; want %q", got, err, aliceNode)
	}
}
