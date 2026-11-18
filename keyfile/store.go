// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package keyfile is the key file: the machine store of a relay without a
// database. Each line is one machine (an id the file assigns, never a
// client's, its name and, when it joined another machine's account, that
// account) and the SHA-256 of the public key its certificates certify
// (Store, Load). A machine joins only with a single-use code (KeyManager
// MintCode, Enrol: the data dir's machine CA signs its certificate); the
// file holds no bearer secret. KeyManager enrols, reloads and revokes
// machines, and Provider authenticates their certificates (nfltr
// docs/decisions/tenants-reach-policy-2026-11-07.md, Decided 7;
// docs/decisions/api-key-cut-2026-11-15.md).
package keyfile

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"

	"strings"
	"sync"

	"github.com/nfltr/relay-core/core/identity"
)

// spkiPrefix marks a line's key: the SHA-256 of the machine key's
// SubjectPublicKeyInfo.
const spkiPrefix = "spki:"

// Machine is one machine of the key file: its id, its name and its
// account (spec/KeyAccount.tla): "" is its own (its id), else the id of
// the first machine of the account it joined, fixed at enrolment. It may
// register any valid service name (its agents are its own id's refs).
type Machine struct {
	ID      identity.MachineID
	Name    string
	Account identity.MachineID
}

// AccountOf is m's account: the principal's owner (Provider), the scope of
// its name (Policy.ResolveMachine).
func (m Machine) AccountOf() identity.MachineID {
	if m.Account == "" {
		return m.ID
	}
	return m.Account
}

// accountField is the optional fourth field of a line: account=<id>.
const accountField = "account="

// ErrMachineName: a machine name the file cannot hold (1 to 63 of
// [a-z0-9-], not starting or ending with '-').
var ErrMachineName = errors.New("keyfile: a machine name is 1 to 63 of [a-z0-9-], not starting or ending with '-'")

// validMachine checks what a line or an enrolment states about a machine.
func validMachine(m Machine) error {
	if _, err := identity.ParseMachineID(string(m.ID)); err != nil {
		return err
	}
	if !identity.ValidMachineName(m.Name) {
		return fmt.Errorf("%w: %q", ErrMachineName, m.Name)
	}
	if m.Account != "" {
		if _, err := identity.ParseMachineID(string(m.Account)); err != nil {
			return fmt.Errorf("account: %w", err)
		}
	}
	return nil
}

// Store holds the machines of a key file by their keys' SHA-256 (the
// SubjectPublicKeyInfo's). It is safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	keys map[[32]byte]Machine // sha256(SPKI) → machine
	tr   tracer               // spec/KeyAccount.tla steps (-tags relaytrace only)
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{keys: make(map[[32]byte]Machine)}
}

// Load reads a key file: one "<machine id> <name> spki:<hex>
// [account=<machine id>]" per non-empty, non-comment line; a name is
// unique in its account. A line of another form (a sha256: API key line:
// keys are gone, docs/decisions/api-key-cut-2026-11-15.md) refuses the
// file: enrol its machines with a code (nfltr-relay machines code). A file
// without lines is an empty store.
func Load(path string) (*Store, error) {
	store, err := read(path)
	if err != nil {
		return nil, err
	}
	store.traceLoad()
	slog.Info("security: loaded the key file", "machines", len(store.keys), "file", path)
	return store, nil
}

// read parses the key file as Load does.
func read(path string) (*Store, error) {
	f, err := os.Open(path) // #nosec G304 -- operator-configured key file
	if err != nil {
		return nil, fmt.Errorf("security: open key file: %w", err)
	}
	defer f.Close() //nolint:errcheck

	store := NewStore()
	type accountName struct {
		account identity.MachineID
		name    string
	}
	ids, names := map[identity.MachineID]bool{}, map[accountName]bool{}
	scanner := bufio.NewScanner(f)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m, h, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("security: %s line %d: %w", path, n, err)
		}
		an := accountName{m.AccountOf(), m.Name}
		if ids[m.ID] || names[an] {
			return nil, fmt.Errorf("security: %s line %d: machine %s (%s) twice: each machine has one key, each name one machine of its account", path, n, m.ID, m.Name)
		}
		ids[m.ID], names[an] = true, true
		store.keys[h] = m
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("security: read key file: %w", err)
	}
	return store, nil
}

// Remove removes the line of machine id.
func (s *Store) Remove(id identity.MachineID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := false
	for key, m := range s.keys {
		if m.ID == id {
			delete(s.keys, key)
			removed = true
		}
	}
	if removed {
		s.trace(event{Ev: "Revoke", M: string(id)})
	}
}

// Count returns the number of machines in the store.
func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keys)
}

// parseLine reads one machine line: "<machine id> <name> spki:<hex>
// [account=<machine id>]".
func parseLine(line string) (Machine, [32]byte, error) {
	f := strings.Fields(line)
	if (len(f) != 3 && (len(f) != 4 || !strings.HasPrefix(f[3], accountField))) || !strings.HasPrefix(f[2], spkiPrefix) {
		return Machine{}, [32]byte{}, errors.New(`want "<machine id> <name> spki:<hex> [account=<machine id>]" (an API key line: keys are gone; enrol the machine with a code, nfltr-relay machines code)`)
	}
	m := Machine{ID: identity.MachineID(f[0]), Name: f[1]}
	if len(f) == 4 {
		m.Account = identity.MachineID(strings.TrimPrefix(f[3], accountField))
		if m.Account == m.ID {
			m.Account = ""
		}
	}
	if err := validMachine(m); err != nil {
		return Machine{}, [32]byte{}, err
	}
	_, hexHash, _ := strings.Cut(f[2], ":")
	h, err := decodeHashPrefix(hexHash)
	return m, h, err
}

// line is m's key file line for the key hash h.
func (m Machine) line(h [32]byte) string {
	l := fmt.Sprintf("%s %s %s%s", m.ID, m.Name, spkiPrefix, hex.EncodeToString(h[:]))
	if m.Account != "" && m.Account != m.ID {
		l += " " + accountField + string(m.Account)
	}
	return l
}

// decodeHashPrefix decodes a 64-char hex string (the portion after "spki:")
// into a [32]byte for storage in Store.keys.
func decodeHashPrefix(hexStr string) ([32]byte, error) {
	b, err := hex.DecodeString(hexStr)
	if err != nil || len(b) != 32 {
		return [32]byte{}, fmt.Errorf("expected 64-char hex, got %q", hexStr)
	}
	var h [32]byte
	copy(h[:], b)
	return h, nil
}

// Hashes is a copy of the store's keys: each key's SHA-256 and its machine.
func (s *Store) Hashes() map[[32]byte]Machine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.keys)
}
