// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
)

// MachineID names a machine: 128 random bits the machine store assigns at
// enrolment (never a client's choice), printed "m-" and 26 lowercase base32
// characters. It is stable across key rotation and never reused; names are
// labels resolved to it at the edge (nfltr docs/decisions/
// tenants-reach-policy-2026-11-07.md "Identity: machine id").
type MachineID string

// MachineIDLen is the length of every MachineID.
const MachineIDLen = 28

// MaxServiceLen bounds a service name so that an agent ref is one DNS
// label (28 + 1 + 34 = 63).
const MaxServiceLen = 34

var machineB32 = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

// ErrMachineNotEnrolled: the credential is known but names no machine (a
// key from before machines, or one whose machine row is gone). Typed so the
// client says how to enrol (`nfltr config login`, the dashboard's Add
// machine) instead of retrying.
var ErrMachineNotEnrolled = errors.New("machine_not_enrolled: this credential names no enrolled machine; enrol with `nfltr config login` or the dashboard's Add machine")

// NewMachineID returns a fresh random id (crypto/rand).
func NewMachineID() MachineID {
	var b [16]byte
	_, _ = rand.Read(b[:]) // never fails (crypto/rand)
	return MachineID("m-" + machineB32.EncodeToString(b[:]))
}

// MachineIDOf is the machine id of a principal a provider identified
// without an enrolment (a self-hosted relay's certificate or token
// subject): derived from what the provider verified (its namespaced
// subject, Key), so the same credential is always the same machine and a
// client never chooses it.
func MachineIDOf(key string) MachineID {
	h := sha256.Sum256([]byte("nfltr machine id\x00" + key))
	return MachineID("m-" + machineB32.EncodeToString(h[:16]))
}

// ParseMachineID accepts exactly the canonical form NewMachineID prints.
func ParseMachineID(s string) (MachineID, error) {
	if len(s) != MachineIDLen || s[:2] != "m-" {
		return "", fmt.Errorf("identity: machine id %q: want m- and 26 base32 characters", s)
	}
	b, err := machineB32.DecodeString(s[2:])
	if err != nil || len(b) != 16 || machineB32.EncodeToString(b) != s[2:] {
		return "", fmt.Errorf("identity: machine id %q: not canonical base32", s)
	}
	return MachineID(s), nil
}

// AgentRef is an agent: one service of one machine (any valid service
// name: the ref is always its machine's own). The same service on two
// machines is two agents.
type AgentRef struct {
	Machine MachineID
	Service string
}

// String is the ref's one canonical form, "<machine id>-<service>": one
// DNS label, the agent id on the wire and in the directory.
func (r AgentRef) String() string { return string(r.Machine) + "-" + r.Service }

// ParseAgentRef splits s at byte MachineIDLen: a fixed-length field, so no
// store is consulted and there is one answer.
func ParseAgentRef(s string) (AgentRef, error) {
	if len(s) < MachineIDLen+2 || s[MachineIDLen] != '-' {
		return AgentRef{}, fmt.Errorf("identity: agent ref %q: want <machine id>-<service>", s)
	}
	m, err := ParseMachineID(s[:MachineIDLen])
	if err != nil {
		return AgentRef{}, err
	}
	if !ValidService(s[MachineIDLen+1:]) {
		return AgentRef{}, fmt.Errorf("identity: agent ref %q: invalid service", s)
	}
	return AgentRef{Machine: m, Service: s[MachineIDLen+1:]}, nil
}

// ValidService reports whether s is a service name: 1 to MaxServiceLen of
// [a-z0-9-], not starting or ending with '-' (the ref stays a DNS label),
// and not itself an agent ref (a machine id and '-'): a ref names one
// machine at a glance, never another's inside it.
func ValidService(s string) bool {
	if len(s) > MachineIDLen && s[MachineIDLen] == '-' {
		if _, err := ParseMachineID(s[:MachineIDLen]); err == nil {
			return false
		}
	}
	return label(s, MaxServiceLen)
}

// ValidMachineName reports whether s is a machine name: one DNS label of
// [a-z0-9-] (a label people type, resolved to a MachineID at the edge).
func ValidMachineName(s string) bool { return label(s, 63) }

func label(s string, maxLen int) bool {
	if len(s) == 0 || len(s) > maxLen || s[0] == '-' || s[len(s)-1] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}
