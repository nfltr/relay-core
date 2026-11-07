// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	keyFile := filepath.Join(t.TempDir(), "api-keys")
	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())
	gen := func(name, member string) *KeyGenerateResponse {
		t.Helper()
		join := km.Generate
		if member != "" {
			join = func(name string) (*KeyGenerateResponse, error) { return km.Join(name, member) }
		}
		r, err := join(name)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	laptop := gen("laptop", "")
	work := gen("work-laptop", "laptop")                // by name
	ci := gen("ci", string(work.MachineID))             // by id: the account, not the member's id
	other := gen("other", "")                           // its own account
	twin := gen("work-laptop", string(other.MachineID)) // a name per account
	if work.Account != laptop.MachineID || ci.Account != laptop.MachineID || other.Account != other.MachineID {
		t.Fatalf("accounts: work %s ci %s other %s, want %s %s %s", work.Account, ci.Account, other.Account, laptop.MachineID, laptop.MachineID, other.MachineID)
	}
	owner := func(key string) string {
		t.Helper()
		p, err := Provider{Store: store}.Authenticate(context.Background(), identity.Credential{Token: key})
		if err != nil {
			t.Fatal(err)
		}
		return p.Owner
	}
	if owner(work.APIKey) != owner(laptop.APIKey) || owner(ci.APIKey) != owner(laptop.APIKey) || owner(other.APIKey) == owner(laptop.APIKey) {
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
	if resolve(laptop.MachineID, "work-laptop") != work.MachineID || resolve(other.MachineID, "work-laptop") != twin.MachineID || resolve(other.MachineID, "ci") != "" {
		t.Fatal("a name resolves within the caller's account only")
	}
	for _, bad := range []string{"desk", string(identity.NewMachineID())} {
		if _, err := km.Join("x", bad); err == nil {
			t.Fatalf("joined %q, a machine the file does not hold", bad)
		}
	}
	if _, err := km.Join("y", "work-laptop"); err == nil || !strings.Contains(err.Error(), "machine id") {
		t.Fatalf("a name two machines hold: %v, want refused for its id", err)
	}

	if err := km.Revoke(laptop.MachineID); err != nil {
		t.Fatal(err)
	}
	back, err := LoadAPIKeys(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := back.Validate(ci.APIKey); !ok || m.AccountOf() != laptop.MachineID || owner(work.APIKey) != owner(ci.APIKey) {
		t.Fatalf("after the first machine's revoke: %+v %v, want account %s kept", m, ok, laptop.MachineID)
	}
	again := gen("laptop", "ci")
	if again.Account != laptop.MachineID || resolve(work.MachineID, "laptop") != again.MachineID {
		t.Fatalf("a revoked machine's name is free at once in its account: %+v", again)
	}

	data, _ := os.ReadFile(keyFile)
	var lines []string
	for _, l := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(l, string(ci.MachineID)+" ") {
			l = strings.Replace(l, " account="+string(laptop.MachineID), " account="+string(other.MachineID), 1)
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
	if owner(work.APIKey) != owner(ci.APIKey) {
		t.Fatal("a refused reload changed an account")
	}
}

// Policy is the session's Policy and NameResolver of a key-file relay.
var (
	_ session.Policy       = Policy{}
	_ session.NameResolver = Policy{}
)
