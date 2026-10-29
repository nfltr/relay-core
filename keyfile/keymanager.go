// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
)

// KeyRecord holds metadata about a generated API key.
type KeyRecord struct {
	AgentID   string    `json:"agent_id"`
	KeyPrefix string    `json:"key_prefix"`      // first 8 chars for identification
	Owner     string    `json:"owner,omitempty"` // email of the user who created this key
	CreatedAt time.Time `json:"created_at"`
}

// KeyGenerateResponse is returned when a new key is generated.
// The full key is only returned ONCE at creation time.
type KeyGenerateResponse struct {
	AgentID   string    `json:"agent_id"`
	APIKey    string    `json:"api_key"`
	CreatedAt time.Time `json:"created_at"`
	Message   string    `json:"message"`
}

// KeyListResponse is returned when listing keys.
type KeyListResponse struct {
	Keys []KeyRecord `json:"keys"`
}

// KeyManager generates, persists, and revokes API keys.
// It manages both the in-memory APIKeyStore and the on-disk key file.
type KeyManager struct {
	mu       sync.Mutex
	store    *APIKeyStore
	filePath string
	records  map[string]KeyRecord // agentID → record
	logger   *slog.Logger
	onRevoke func(agentID string)
}

// OnRevoke sets what Revoke and Reload call after they removed a key (the
// relay ends the key's live connections).
func (km *KeyManager) OnRevoke(fn func(agentID string)) {
	km.mu.Lock()
	defer km.mu.Unlock()
	km.onRevoke = fn
}

// NewKeyManager creates a key manager backed by the given file and live store.
// If the store is nil, a new empty store is created.
func NewKeyManager(filePath string, store *APIKeyStore, logger *slog.Logger) *KeyManager {
	if store == nil {
		store = NewAPIKeyStore()
	}
	km := &KeyManager{
		store:    store,
		filePath: filePath,
		records:  make(map[string]KeyRecord),
		logger:   logger,
	}
	// Populate records from existing store keys (no prefix info available for pre-existing keys)
	store.mu.RLock()
	for key, agentID := range store.keys {
		km.records[agentID] = fileRecord(agentID, key)
	}
	store.mu.RUnlock()
	return km
}

// fileRecord is the record of a key read from the file: its hash's prefix,
// no owner, creation time unknown.
func fileRecord(agentID string, hash [32]byte) KeyRecord {
	return KeyRecord{AgentID: agentID, KeyPrefix: hex.EncodeToString(hash[:4]) + "..."}
}

// Generate creates a new API key for the given AgentID.
// Returns the full key (only shown once). The key is immediately usable
// for authentication and is persisted to the key file.
func (km *KeyManager) Generate(agentID string) (*KeyGenerateResponse, error) {
	return km.GenerateForOwner(agentID, "")
}

// GenerateForOwner creates a new API key for the given AgentID,
// recording the owner email for per-user scoping.
func (km *KeyManager) GenerateForOwner(agentID, owner string) (*KeyGenerateResponse, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agentID is required")
	}
	// The key file holds agentID:sha256:<hex> per line (LoadAPIKeys): an ID
	// it cannot round-trip would re-key or add an agent, or stop the file
	// loading, on the next start.
	if strings.HasPrefix(agentID, "#") || strings.ContainsFunc(agentID, func(r rune) bool {
		return r == ':' || unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
		return nil, fmt.Errorf("agentID %q must not start with '#' or contain ':', spaces or control characters", agentID)
	}

	km.mu.Lock()
	defer km.mu.Unlock()

	// Check if AgentID already has a key
	if _, exists := km.records[agentID]; exists {
		return nil, fmt.Errorf("agentID %q already has an API key; revoke it first", agentID)
	}

	// Generate a cryptographically random key: rpc_ + 48 random bytes (96 hex chars)
	keyBytes := make([]byte, 48)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("generate random key: %w", err)
	}
	apiKey := "rpc_" + hex.EncodeToString(keyBytes)

	// Add to live store
	km.store.Add(agentID, apiKey)

	// Persist to file
	if err := km.appendToFile(agentID, apiKey); err != nil {
		// Roll back from live store on file error
		km.store.Remove(agentID)
		return nil, fmt.Errorf("persist key: %w", err)
	}

	record := KeyRecord{
		AgentID:   agentID,
		KeyPrefix: apiKey[:12] + "...",
		Owner:     owner,
		CreatedAt: time.Now().UTC(),
	}
	km.records[agentID] = record

	km.logger.Info("security: generated API key", "agent_id", agentID, "owner", owner, "prefix", record.KeyPrefix)

	return &KeyGenerateResponse{
		AgentID:   agentID,
		APIKey:    apiKey,
		CreatedAt: record.CreatedAt,
		Message:   "Store this key securely — it will not be shown again.",
	}, nil
}

// Revoke removes the API key for the given AgentID from both the live store
// and the on-disk key file.
func (km *KeyManager) Revoke(agentID string) error {
	if agentID == "" {
		return fmt.Errorf("agentID is required")
	}

	km.mu.Lock()
	defer km.mu.Unlock()

	if _, exists := km.records[agentID]; !exists {
		return fmt.Errorf("no API key found for agentID %q", agentID)
	}

	// Remove from live store and end its connections, whether or not the
	// file below is rewritten.
	km.store.Remove(agentID)
	if km.onRevoke != nil {
		km.onRevoke(agentID)
	}
	delete(km.records, agentID)

	// Rewrite file without this AgentID
	if err := km.rewriteFile(); err != nil {
		return fmt.Errorf("persist revocation: %w", err)
	}

	km.logger.Info("security: revoked API key", "agent_id", agentID)
	return nil
}

// List returns metadata about all managed keys (never exposes the actual keys).
func (km *KeyManager) List() []KeyRecord {
	km.mu.Lock()
	defer km.mu.Unlock()

	records := make([]KeyRecord, 0, len(km.records))
	for _, r := range km.records {
		records = append(records, r)
	}
	return records
}

// ListForOwner returns keys owned by the given email address.
func (km *KeyManager) ListForOwner(email string) []KeyRecord {
	km.mu.Lock()
	defer km.mu.Unlock()

	records := make([]KeyRecord, 0)
	for _, r := range km.records {
		if r.Owner == email {
			records = append(records, r)
		}
	}
	return records
}

// IsOwnedBy checks whether the given AgentID's key was created by the given email.
func (km *KeyManager) IsOwnedBy(agentID, email string) bool {
	km.mu.Lock()
	defer km.mu.Unlock()
	r, exists := km.records[agentID]
	return exists && r.Owner == email
}

// appendToFile appends a single hashed key entry to the key file.
// Format: agentID:sha256:<hex> — plaintext key is never written to disk.
func (km *KeyManager) appendToFile(agentID, key string) error {
	if km.filePath == "" {
		return nil // no file persistence (e.g., env-only mode)
	}
	data, err := os.ReadFile(km.filePath) // #nosec G304 -- operator-configured API key file
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	data = fmt.Appendf(data, "%s:%s%s\n", agentID, hashPrefix, hashKey(key))
	return writeFileAtomic(km.filePath, data)
}

// rewriteFile rebuilds the key file from the current store contents.
// Keys are written in hashed format: agentID:sha256:<hex>
func (km *KeyManager) rewriteFile() error {
	if km.filePath == "" {
		return nil
	}

	km.store.mu.RLock()
	var lines []string
	lines = append(lines, "# API keys managed by the relay (do not edit manually)")
	lines = append(lines, "# Format: agentID:sha256:<hex_hash>")
	for hash, agentID := range km.store.keys {
		lines = append(lines, fmt.Sprintf("%s:%s%s", agentID, hashPrefix, hex.EncodeToString(hash[:])))
	}
	km.store.mu.RUnlock()

	return writeFileAtomic(km.filePath, []byte(strings.Join(lines, "\n")+"\n"))
}

// ---------------------------------------------------------------------------
// HTTP Handlers — register on the internal port (not exposed externally)
// ---------------------------------------------------------------------------
