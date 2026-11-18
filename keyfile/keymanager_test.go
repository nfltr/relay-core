// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

func testKeyManagerLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// TestKeyFileAssignsMachineID (Decided 7): enrolling a machine assigns it
// a fresh id the caller never chose; its line binds its key's SPKI hash,
// and the file loads back the same machine. An invalid name is refused,
// and a later machine never gets an id again.
func TestKeyFileAssignsMachineID(t *testing.T) {
	km, _, ca := certManager(t)
	east := enrol(t, km, ca, "east", "")
	if _, err := identity.ParseMachineID(string(east.ID)); err != nil || east.Name != "east" {
		t.Fatalf("enrolled %+v (%v)", east.Machine, err)
	}
	data, _ := os.ReadFile(km.filePath)
	if !strings.Contains(string(data), string(east.ID)+" east spki:") {
		t.Fatalf("key file %q: want the machine's spki line", data)
	}
	back, err := Load(km.filePath)
	if m, ok := byID(back, east.ID); err != nil || !ok || m.Name != "east" {
		t.Fatalf("reloaded: %+v %v %v", m, ok, err)
	}
	for _, bad := range []string{"", "West", "a:b", "-west"} {
		if _, err := km.MintCode(bad, "", time.Hour, time.Now()); err == nil {
			t.Fatalf("MintCode(%q) was accepted", bad)
		}
	}
	if west := enrol(t, km, ca, "west", ""); west.ID == east.ID {
		t.Fatalf("second machine %+v", west.Machine)
	}
	if len(km.List()) != 2 {
		t.Fatalf("%d records, want 2", len(km.List()))
	}
}

func TestKeyManagerRevoke(t *testing.T) {
	km, p, ca := certManager(t)
	var cut []string
	km.OnRevoke(func(s string) { cut = append(cut, s) })
	east := enrol(t, km, ca, "east", "")
	if err := km.Revoke(east.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := certPrincipal(p, east.chain); err == nil || len(cut) != 1 || cut[0] != string(east.ID) {
		t.Fatalf("after revoke: %v, OnRevoke %v", err, cut)
	}
	if data, _ := os.ReadFile(km.filePath); strings.Contains(string(data), string(east.ID)) {
		t.Fatal("the key file still holds the revoked machine")
	}
	if err := km.Revoke(east.ID); err == nil {
		t.Fatal("a second revoke of the machine: no error")
	}
	// Its name is free again; the new machine gets a new id.
	if again := enrol(t, km, ca, "east", ""); again.ID == east.ID {
		t.Fatalf("re-added: %+v", again.Machine)
	}
}

// The provider states the certificate's machine: its id and its own
// account; it registers any valid service name as its own ref (spec
// Reach.tla AgentIdOwned), nothing else.
func TestProviderStatesTheMachine(t *testing.T) {
	km, p, ca := certManager(t)
	east := enrol(t, km, ca, "east", "")
	a := identity.NewAuthenticator(ProviderName, p)
	pr, err := a.Authenticate(context.Background(), identity.Credential{PeerCerts: east.chain, Name: "ssh"})
	if err != nil || pr.Machine != east.ID || pr.AgentID != (identity.AgentRef{Machine: east.ID, Service: "ssh"}).String() {
		t.Fatalf("principal %+v, %v", pr, err)
	}
	if pr, err := a.Authenticate(context.Background(), identity.Credential{PeerCerts: east.chain, Name: "web"}); err != nil || pr.AgentID != (identity.AgentRef{Machine: east.ID, Service: "web"}).String() {
		t.Fatalf("another service: %+v, %v", pr, err)
	}
	if _, err := a.Authenticate(context.Background(), identity.Credential{PeerCerts: east.chain, Name: "Web"}); !errors.Is(err, identity.ErrNameNotAllowed) {
		t.Fatalf("an invalid service name: %v", err)
	}
}
