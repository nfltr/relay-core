// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package keyfile is the static API key file: a store of hashed keys by
// agent id (APIKeyStore, LoadAPIKeys), its reload and key minting
// (KeyManager), and the identity provider over it (Provider) that
// authenticates a relay's agents and clients without a database.
package keyfile

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strings"
	"sync"
)

const hashPrefix = "sha256:"

// APIKeyStore holds a set of valid API keys loaded from a file.
// Each key maps to an AgentID — the identity of the agent or client.
// Keys are stored as raw SHA-256 hashes ([32]byte) in memory — plaintext and
// hex representations are never retained — so Validate pays only one SHA-256
// call and one map lookup per request (no hex encoding, no linear scan).
// The file format supports both legacy ("agentID:secret_key") and
// hashed ("agentID:sha256:<hex>") entries.
//
// It is safe for concurrent use.
type APIKeyStore struct {
	mu   sync.RWMutex
	keys map[[32]byte]string // sha256(secret_key) → agentID
}

// NewAPIKeyStore returns an empty key store.
func NewAPIKeyStore() *APIKeyStore {
	return &APIKeyStore{keys: make(map[[32]byte]string)}
}

// LoadAPIKeys reads API keys from a file. Each non-empty, non-comment line
// must have the format "agentID:secret_key" (legacy) or "agentID:sha256:<hex>"
// (hashed). Legacy plaintext keys are hashed on load so memory never holds
// plaintext credentials.
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
	agentIDSeen := make(map[string]bool) // detect duplicate AgentIDs
	var legacyCount int
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		agentID, key := parseLine(line)
		if agentIDSeen[agentID] {
			return nil, fmt.Errorf("security: duplicate AgentID %q in %s — each API key must map to a unique agent", agentID, path)
		}
		agentIDSeen[agentID] = true

		// Support both hashed (sha256:hex) and legacy plaintext formats.
		if strings.HasPrefix(key, hashPrefix) {
			h, err := decodeHashPrefix(key[len(hashPrefix):])
			if err != nil {
				return nil, fmt.Errorf("security: malformed sha256 hash for agent %q in %s: %w", agentID, path, err)
			}
			store.keys[h] = agentID
		} else {
			store.keys[sha256.Sum256([]byte(key))] = agentID
			legacyCount++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("security: read API key file: %w", err)
	}
	if legacyCount > 0 {
		slog.Warn("security: loaded legacy plaintext API keys — regenerate keys to store hashed",
			"legacy_count", legacyCount, "file", path)
	}
	return store, nil
}

// LoadAPIKeysFromEnv loads a single API key from an environment variable.
// The env var value must be in the format "agentID:secret_key".
// If no colon is present, the env var name is used as the AgentID (legacy).
// The key is hashed before storing in memory.
func LoadAPIKeysFromEnv(envVar string) (*APIKeyStore, error) {
	val := os.Getenv(envVar)
	if val == "" {
		return nil, fmt.Errorf("security: env var %s is empty", envVar)
	}
	store := NewAPIKeyStore()
	agentID, key := parseLine(val)
	store.keys[sha256.Sum256([]byte(key))] = agentID
	return store, nil
}

// Add hashes the key and adds it to the store with the given AgentID.
// The plaintext key is never stored in memory.
func (s *APIKeyStore) Add(agentID, key string) {
	h := sha256.Sum256([]byte(key))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[h] = agentID
}

// Remove removes all keys associated with the given AgentID.
func (s *APIKeyStore) Remove(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, id := range s.keys {
		if id == agentID {
			delete(s.keys, key)
		}
	}
}

// Validate checks whether the given key is in the store.
// Returns the AgentID associated with the key on success.
//
// The candidate is hashed with SHA-256 before the lock is acquired;
// the map is then looked up directly in O(1).  Comparing SHA-256 digests via
// a map lookup does not leak information about the underlying key — a timing
// side-channel on hash comparison cannot be reversed through SHA-256
// pre-image resistance, so a constant-time loop is unnecessary here.
func (s *APIKeyStore) Validate(candidate string) (agentID string, ok bool) {
	h := sha256.Sum256([]byte(candidate))
	s.mu.RLock()
	defer s.mu.RUnlock()
	agentID, ok = s.keys[h]
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

// ScopedAgentID returns the agent identity scoped to the key's namespace.
// When the agent supplies a custom name (--name) that differs from the key's
// stored agentID, the result is "keyAgentID.override" — ensuring names are
// unique per authentication credential rather than globally.
// If override is empty or matches keyAgentID, keyAgentID is returned as-is.
func ScopedAgentID(keyAgentID, override string) string {
	if override == "" || override == keyAgentID {
		return keyAgentID
	}
	// If override is already scoped (e.g. the agent sends back its
	// x-resolved-agent-id), don't add the prefix a second time.
	if strings.HasPrefix(override, keyAgentID+".") {
		return override
	}
	return keyAgentID + "." + override
}

func parseLine(line string) (agentID, key string) {
	if idx := strings.IndexByte(line, ':'); idx > 0 {
		return strings.TrimSpace(line[:idx]), strings.TrimSpace(line[idx+1:])
	}
	return line, line
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

// Hashes is a copy of the store's keys: each key's SHA-256 and its agent.
func (s *APIKeyStore) Hashes() map[[32]byte]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.keys)
}

// hashKey returns the hex-encoded SHA-256 hash of key.
func hashKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
