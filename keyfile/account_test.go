// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
)

// TestKeyFileAccounts (spec/KeyAccount.tla): `keys generate --account`
// joins a machine the file holds, so the machines of one account share the
// provider's owner (SameAccountReaches) and resolve each other's names,
// never another account's (ResolveOwnAccount); joining a machine the file
// does not hold is refused (AccountRooted); revoking the account's first
// machine leaves the account to the others, its name free at once
// (WitnessOutlivesFirst, WitnessNameReused); a reload that changes a kept
// machine's account changes nothing (AccountStable); a line round-trips.
func TestKeyFileAccounts(t *testing.T) {
	km, p, ca := certManager(t)
	keyFile, store := km.filePath, km.store
	gen := func(name, member string) enrolled { t.Helper(); return enrol(t, km, ca, name, member) }
	laptop := gen("laptop", "")
	work := gen("work-laptop", "laptop")         // by name
	ci := gen("ci", string(work.ID))             // by id: the account, not the member's id
	other := gen("other", "")                    // its own account
	twin := gen("work-laptop", string(other.ID)) // a name per account
	if work.AccountOf() != laptop.ID || ci.AccountOf() != laptop.ID || other.AccountOf() != other.ID {
		t.Fatalf("accounts: work %s ci %s other %s, want %s %s %s", work.AccountOf(), ci.AccountOf(), other.AccountOf(), laptop.ID, laptop.ID, other.ID)
	}
	owner := func(e enrolled) string {
		t.Helper()
		pr, err := certPrincipal(p, e.chain)
		if err != nil {
			t.Fatal(err)
		}
		return pr.Owner
	}
	if owner(work) != owner(laptop) || owner(ci) != owner(laptop) || owner(other) == owner(laptop) {
		t.Fatal("one account, one owner; another account, another")
	}
	resolve := func(caller identity.MachineID, name string) identity.MachineID {
		t.Helper()
		m, err := Policy{Store: store}.ResolveMachine(identity.Principal{Provider: ProviderName, Machine: caller}, name)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	if resolve(laptop.ID, "work-laptop") != work.ID || resolve(other.ID, "work-laptop") != twin.ID || resolve(other.ID, "ci") != "" {
		t.Fatal("a name resolves within the caller's account only")
	}
	for _, bad := range []string{"desk", string(identity.NewMachineID())} {
		if _, err := km.MintCode("x", bad, time.Hour, time.Now()); err == nil {
			t.Fatalf("a code joins %q, a machine the file does not hold", bad)
		}
	}
	if _, err := km.MintCode("y", "work-laptop", time.Hour, time.Now()); err == nil || !strings.Contains(err.Error(), "machine id") {
		t.Fatalf("a name two machines hold: %v, want refused for its id", err)
	}

	if err := km.Revoke(laptop.ID); err != nil {
		t.Fatal(err)
	}
	back, err := Load(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := byID(back, ci.ID); !ok || m.AccountOf() != laptop.ID || owner(work) != owner(ci) {
		t.Fatalf("after the first machine's revoke: %+v %v, want account %s kept", m, ok, laptop.ID)
	}
	again := gen("laptop", "ci")
	if again.AccountOf() != laptop.ID || resolve(work.ID, "laptop") != again.ID {
		t.Fatalf("a revoked machine's name is free at once in its account: %+v", again)
	}

	data, _ := os.ReadFile(keyFile)
	var lines []string
	for _, l := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(l, string(ci.ID)+" ") {
			l = strings.Replace(l, " account="+string(laptop.ID), " account="+string(other.ID), 1)
		}
		lines = append(lines, l)
	}
	edited := strings.Join(lines, "\n")
	if edited == string(data) {
		t.Fatalf("no account field on ci's line in %q", data)
	}
	if err := os.WriteFile(keyFile, []byte(edited), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := km.Reload(); err == nil || !strings.Contains(err.Error(), "account changed") {
		t.Fatalf("a reload moving a machine to another account: %v, want refused", err)
	}
	if owner(work) != owner(ci) {
		t.Fatal("a refused reload changed an account")
	}
}

// Policy is the session's Policy and NameResolver of a key-file relay.
var (
	_ session.Policy       = Policy{}
	_ session.NameResolver = Policy{}
)
