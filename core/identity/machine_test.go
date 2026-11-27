// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"strings"
	"testing"
)

func TestMachineIDRandom(t *testing.T) {
	seen := make(map[MachineID]bool, 100000)
	for range 100000 {
		id := NewMachineID()
		if seen[id] {
			t.Fatalf("repeated id %s", id)
		}
		seen[id] = true
		if got, err := ParseMachineID(string(id)); err != nil || got != id {
			t.Fatalf("ParseMachineID(%s) = %s, %v", id, got, err)
		}
	}
}

func TestAgentRefRoundTrip(t *testing.T) {
	m := NewMachineID()
	for _, svc := range []string{"a", "ssh", "web-1", strings.Repeat("x", MaxServiceLen), "0db"} {
		r := AgentRef{Machine: m, Service: svc}
		got, err := ParseAgentRef(r.String())
		if err != nil || got != r {
			t.Fatalf("ParseAgentRef(%s) = %+v, %v", r, got, err)
		}
		if len(r.String()) > 63 {
			t.Fatalf("ref %s is longer than a DNS label", r)
		}
	}
	bad := []string{
		"", string(m), string(m) + "-", string(m) + "ssh", // no service, no separator
		string(m)[:27] + "-ssh",             // short id
		"x" + string(m)[1:] + "-ssh",        // wrong prefix
		strings.ToUpper(string(m)) + "-ssh", // upper case
		string(m)[:27] + "1-ssh",            // a character outside base32
		string(m) + "-Ssh", string(m) + "-s_h", string(m) + "-s.h", string(m) + "--ssh", string(m) + "-ssh-",
		string(m) + "-" + strings.Repeat("x", MaxServiceLen+1), // a 35-character service
	}
	for _, s := range bad {
		if r, err := ParseAgentRef(s); err == nil {
			t.Fatalf("ParseAgentRef(%q) = %+v, want an error", s, r)
		}
	}
	// The last character carries 2 unused bits: only the canonical form parses.
	if _, err := ParseMachineID("m-" + strings.Repeat("a", 26)); err != nil {
		t.Fatal(err)
	}
	if id, err := ParseMachineID("m-" + strings.Repeat("a", 25) + "b"); err == nil {
		t.Fatalf("non-canonical id %s parsed", id)
	}
}

// TestMachineIDOfStableCanonical: a derived id parses and is the same for
// the same key, different for another.
func TestMachineIDOfStableCanonical(t *testing.T) {
	a, b := MachineIDOf("mtls:alice"), MachineIDOf("mtls:bob")
	if _, err := ParseMachineID(string(a)); err != nil {
		t.Fatal(err)
	}
	if a != MachineIDOf("mtls:alice") || a == b {
		t.Fatalf("MachineIDOf: %s %s", a, b)
	}
}
