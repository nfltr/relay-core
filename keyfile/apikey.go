// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package keyfile is the static API key file: the machine store of a relay
// without a database. Each line is one machine (an id the file assigns,
// never a client's, its name and bound services) and the SHA-256 of its
// key (APIKeyStore, LoadAPIKeys); KeyManager adds, reloads and revokes
// machines, and Provider authenticates them (nfltr
// docs/decisions/tenants-reach-policy-2026-11-07.md, Decided 7).
package keyfile

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/nfltr/relay-core/core/identity"
)

const hashPrefix = "sha256:"

// Machine is one machine of the key file: its id, name and bound
// services (the agents it may register).
type Machine struct {
	ID       identity.MachineID
	Name     string
	Services []string
}

// ErrMachineName: a machine name the file cannot hold (1 to 63 of
// [a-z0-9-], not starting or ending with '-').
var ErrMachineName = errors.New("keyfile: a machine name is 1 to 63 of [a-z0-9-], not starting or ending with '-'")

// ErrServiceName: a service name over identity.MaxServiceLen characters or
// outside [a-z0-9-] (an agent ref must stay one DNS label).
var ErrServiceName = fmt.Errorf("keyfile: a service name is 1 to %d of [a-z0-9-], not starting or ending with '-'", identity.MaxServiceLen)

// validMachine checks what a line or a Generate states about a machine.
func validMachine(m Machine) error {
	if _, err := identity.ParseMachineID(string(m.ID)); err != nil {
		return err
	}
	if !identity.ValidMachineName(m.Name) {
		return fmt.Errorf("%w: %q", ErrMachineName, m.Name)
	}
	if len(m.Services) == 0 {
		return fmt.Errorf("keyfile: machine %s binds no service", m.Name)
	}
	for i, svc := range m.Services {
		if !identity.ValidService(svc) {
			return fmt.Errorf("%w: %q", ErrServiceName, svc)
		}
		if slices.Contains(m.Services[:i], svc) {
			return fmt.Errorf("keyfile: machine %s binds %q twice", m.Name, svc)
		}
	}
	return nil
}

// APIKeyStore holds the machines of a key file by their keys' SHA-256:
// plaintext and hex keys are never retained, so Validate pays one SHA-256
// and one map lookup. It is safe for concurrent use.
type APIKeyStore struct {
	mu   sync.RWMutex
	keys map[[32]byte]Machine // sha256(key) → machine
}

// NewAPIKeyStore returns an empty key store.
func NewAPIKeyStore() *APIKeyStore {
	return &APIKeyStore{keys: make(map[[32]byte]Machine)}
}

// LoadAPIKeys reads a key file: one "<machine id> <name> <service,...>
// sha256:<hex>" per non-empty, non-comment line. A line of another form
// (the agent-id files before machines) refuses the file: re-add its
// machines with `nfltr-relay keys add`.
func LoadAPIKeys(path string) (*APIKeyStore, error) {
	store, err := readAPIKeys(path)
	if err != nil {
		return nil, err
	}
	if len(store.keys) == 0 {
		return nil, fmt.Errorf("security: no API keys found in %s", path)
	}
	slog.Info("security: loaded API keys", "count", len(store.keys), "file", path)
	return store, nil
}

// readAPIKeys parses the key file as LoadAPIKeys does; a file without keys
// (empty, or comments only) is an empty store.
func readAPIKeys(path string) (*APIKeyStore, error) {
	f, err := os.Open(path) // #nosec G304 -- operator-configured API key file
	if err != nil {
		return nil, fmt.Errorf("security: open API key file: %w", err)
	}
	defer f.Close() //nolint:errcheck

	store := NewAPIKeyStore()
	ids, names := map[identity.MachineID]bool{}, map[string]bool{}
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
		if ids[m.ID] || names[m.Name] {
			return nil, fmt.Errorf("security: %s line %d: machine %s (%s) twice: each machine has one key", path, n, m.ID, m.Name)
		}
		ids[m.ID], names[m.Name] = true, true
		store.keys[h] = m
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("security: read API key file: %w", err)
	}
	return store, nil
}

// LoadAPIKeysFromEnv loads one machine from an environment variable:
// "<machine id> <name> <service,...> <key>". The key is hashed before it
// is stored.
func LoadAPIKeysFromEnv(envVar string) (*APIKeyStore, error) {
	val := os.Getenv(envVar)
	if val == "" {
		return nil, fmt.Errorf("security: env var %s is empty", envVar)
	}
	f := strings.Fields(val)
	if len(f) != 4 {
		return nil, fmt.Errorf("security: env var %s: want \"<machine id> <name> <service,...> <key>\"", envVar)
	}
	m := Machine{ID: identity.MachineID(f[0]), Name: f[1], Services: strings.Split(f[2], ",")}
	if err := validMachine(m); err != nil {
		return nil, fmt.Errorf("security: env var %s: %w", envVar, err)
	}
	store := NewAPIKeyStore()
	store.keys[sha256.Sum256([]byte(f[3]))] = m
	return store, nil
}

// Add hashes the key and adds it to the store for machine m. The plaintext
// key is never stored in memory.
func (s *APIKeyStore) Add(m Machine, key string) {
	h := sha256.Sum256([]byte(key))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[h] = m
}

// Remove removes the keys of machine id.
func (s *APIKeyStore) Remove(id identity.MachineID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, m := range s.keys {
		if m.ID == id {
			delete(s.keys, key)
		}
	}
}

// Validate checks whether the given key is in the store and returns its
// machine.
//
// The candidate is hashed with SHA-256 before the lock is acquired;
// the map is then looked up directly in O(1).  Comparing SHA-256 digests via
// a map lookup does not leak information about the underlying key — a timing
// side-channel on hash comparison cannot be reversed through SHA-256
// pre-image resistance, so a constant-time loop is unnecessary here.
func (s *APIKeyStore) Validate(candidate string) (m Machine, ok bool) {
	h := sha256.Sum256([]byte(candidate))
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok = s.keys[h]
	return
}

// HasKeyHash reports whether the key with hex SHA-256 keyHash is in the
// store (a session minted from it is still backed by a live key).
func (s *APIKeyStore) HasKeyHash(keyHash string) bool {
	var h [32]byte
	if len(keyHash) != 2*len(h) {
		return false
	}
	if _, err := hex.Decode(h[:], []byte(keyHash)); err != nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.keys[h]
	return ok
}

// Count returns the number of keys in the store.
func (s *APIKeyStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.keys)
}

// parseLine reads one machine line: "<machine id> <name> <service,...>
// sha256:<hex>".
func parseLine(line string) (Machine, [32]byte, error) {
	f := strings.Fields(line)
	if len(f) != 4 || !strings.HasPrefix(f[3], hashPrefix) {
		return Machine{}, [32]byte{}, errors.New(`want "<machine id> <name> <service,...> sha256:<hex>" (a key file from before machines: re-add each machine with nfltr-relay keys add)`)
	}
	m := Machine{ID: identity.MachineID(f[0]), Name: f[1], Services: strings.Split(f[2], ",")}
	if err := validMachine(m); err != nil {
		return Machine{}, [32]byte{}, err
	}
	h, err := decodeHashPrefix(f[3][len(hashPrefix):])
	return m, h, err
}

// Line is m's key file line for key (its hash; the key itself is never
// stored).
func (m Machine) Line(key string) string { return m.line(sha256.Sum256([]byte(key))) }

// line is m's key file line for the key hash h.
func (m Machine) line(h [32]byte) string {
	return fmt.Sprintf("%s %s %s %s%s", m.ID, m.Name, strings.Join(m.Services, ","), hashPrefix, hex.EncodeToString(h[:]))
}

// decodeHashPrefix decodes a 64-char hex string (the portion after "sha256:")
// into a [32]byte for storage in APIKeyStore.keys.
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
func (s *APIKeyStore) Hashes() map[[32]byte]Machine {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.keys)
}
