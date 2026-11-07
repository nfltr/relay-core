// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
)

func testKeyManagerLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestKeyFileAssignsMachineID (Decided 7): adding a machine assigns it a
// fresh id the caller never chose; the key validates at once as that
// machine, is written hashed only, and the file loads back the same
// machine. Two machines of one name are refused, a service over 34
// characters is refused typed, and a later machine never gets an id again.
func TestKeyFileAssignsMachineID(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "api-keys")
	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())

	resp, err := km.Generate("east", []string{"ssh", "web"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identity.ParseMachineID(string(resp.MachineID)); err != nil || resp.Name != "east" {
		t.Fatalf("Generate = %+v (%v)", resp, err)
	}
	if !strings.HasPrefix(resp.APIKey, "rpc_") || len(resp.APIKey) != 100 {
		t.Fatalf("key %q: want rpc_ and 96 hex characters", resp.APIKey)
	}
	if m, ok := store.Validate(resp.APIKey); !ok || m.ID != resp.MachineID {
		t.Fatalf("the new key validates as %+v, %v", m, ok)
	}
	data, _ := os.ReadFile(keyFile)
	if strings.Contains(string(data), resp.APIKey) || !strings.Contains(string(data), string(resp.MachineID)+" east ssh,web sha256:") {
		t.Fatalf("key file %q: want the machine's hashed line only", data)
	}
	back, err := LoadAPIKeys(keyFile)
	if m, ok := back.Validate(resp.APIKey); err != nil || !ok || m.ID != resp.MachineID {
		t.Fatalf("reloaded: %+v %v %v", m, ok, err)
	}

	if _, err := km.Generate("east", []string{"ssh"}); err == nil {
		t.Fatal("a second machine named east was added")
	}
	if _, err := km.Generate("west", []string{strings.Repeat("s", identity.MaxServiceLen+1)}); !errors.Is(err, ErrServiceName) {
		t.Fatalf("a 35-character service: %v, want ErrServiceName", err)
	}
	for _, bad := range []struct {
		name string
		svcs []string
	}{{"", []string{"ssh"}}, {"West", []string{"ssh"}}, {"a:b", []string{"ssh"}}, {"west", nil}, {"west", []string{"ssh", "ssh"}}} {
		if _, err := km.Generate(bad.name, bad.svcs); err == nil {
			t.Fatalf("Generate(%q, %v) was accepted", bad.name, bad.svcs)
		}
	}
	other, err := km.Generate("west", []string{"ssh"})
	if err != nil || other.MachineID == resp.MachineID {
		t.Fatalf("second machine %+v, %v", other, err)
	}
	if len(km.List()) != 2 {
		t.Fatalf("%d records, want 2", len(km.List()))
	}
}

func TestKeyManagerRevoke(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "api-keys")
	store := NewAPIKeyStore()
	km := NewKeyManager(keyFile, store, testKeyManagerLogger())
	var cut []string
	km.OnRevoke(func(s string) { cut = append(cut, s) })
	resp, err := km.Generate("east", []string{"ssh"})
	if err != nil {
		t.Fatal(err)
	}
	if err := km.Revoke(resp.MachineID); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Validate(resp.APIKey); ok || len(cut) != 1 || cut[0] != string(resp.MachineID) {
		t.Fatalf("after revoke: valid %v, OnRevoke %v", ok, cut)
	}
	if data, _ := os.ReadFile(keyFile); strings.Contains(string(data), string(resp.MachineID)) {
		t.Fatal("the key file still holds the revoked machine")
	}
	if err := km.Revoke(resp.MachineID); err == nil {
		t.Fatal("a second revoke of the machine: no error")
	}
	// Its name is free again; the new machine gets a new id.
	again, err := km.Generate("east", []string{"ssh"})
	if err != nil || again.MachineID == resp.MachineID {
		t.Fatalf("re-added: %+v, %v", again, err)
	}
}

// The provider states the key's machine: its id, its services as the names
// it may register, and its own account.
func TestProviderStatesTheMachine(t *testing.T) {
	store := NewAPIKeyStore()
	km := NewKeyManager("", store, testKeyManagerLogger())
	resp, err := km.Generate("east", []string{"ssh"})
	if err != nil {
		t.Fatal(err)
	}
	a := identity.NewAuthenticator(ProviderName, Provider{Store: store})
	p, err := a.Authenticate(context.Background(), identity.Credential{Token: resp.APIKey, Name: "ssh"})
	if err != nil || p.Machine != resp.MachineID || p.AgentID != (identity.AgentRef{Machine: resp.MachineID, Service: "ssh"}).String() {
		t.Fatalf("principal %+v, %v", p, err)
	}
	if _, err := a.Authenticate(context.Background(), identity.Credential{Token: resp.APIKey, Name: "web"}); !errors.Is(err, identity.ErrNameNotAllowed) {
		t.Fatalf("an unbound service: %v", err)
	}
}
