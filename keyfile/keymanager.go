// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// KeyRecord holds metadata about a machine's key.
type KeyRecord struct {
	MachineID identity.MachineID `json:"machine_id"`
	Name      string             `json:"name"`
	Services  []string           `json:"services"`
	KeyPrefix string             `json:"key_prefix"`      // first chars for identification
	Owner     string             `json:"owner,omitempty"` // email of the user who created this key
	CreatedAt time.Time          `json:"created_at"`
}

// KeyGenerateResponse is returned when a machine is added. The full key is
// only returned ONCE, at creation time.
type KeyGenerateResponse struct {
	MachineID identity.MachineID `json:"machine_id"`
	Name      string             `json:"name"`
	Services  []string           `json:"services"`
	APIKey    string             `json:"api_key"`
	CreatedAt time.Time          `json:"created_at"`
	Message   string             `json:"message"`
}

// KeyListResponse is returned when listing keys.
type KeyListResponse struct {
	Keys []KeyRecord `json:"keys"`
}

// KeyManager adds, persists and revokes the key file's machines. It
// manages both the in-memory APIKeyStore and the on-disk key file.
type KeyManager struct {
	mu       sync.Mutex
	store    *APIKeyStore
	filePath string
	records  map[identity.MachineID]KeyRecord
	logger   *slog.Logger
	onRevoke func(subject string)
}

// OnRevoke sets what Revoke and Reload call after they removed a machine's
// key, with its subject (the machine id: the relay ends its connections).
func (km *KeyManager) OnRevoke(fn func(subject string)) {
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
		records:  make(map[identity.MachineID]KeyRecord),
		logger:   logger,
	}
	store.mu.RLock()
	for key, m := range store.keys {
		km.records[m.ID] = fileRecord(m, key)
	}
	store.mu.RUnlock()
	return km
}

// fileRecord is the record of a key read from the file: its hash's prefix,
// no owner, creation time unknown.
func fileRecord(m Machine, hash [32]byte) KeyRecord {
	return KeyRecord{MachineID: m.ID, Name: m.Name, Services: m.Services, KeyPrefix: hex.EncodeToString(hash[:4]) + "..."}
}

// Generate adds a machine named name, bound to services: the file assigns
// it a fresh id (identity.NewMachineID; a caller never chooses one).
// Returns the full key (only shown once), usable at once and persisted.
func (km *KeyManager) Generate(name string, services []string) (*KeyGenerateResponse, error) {
	return km.GenerateForOwner(name, services, "")
}

// GenerateForOwner is Generate recording the owner email for per-user
// scoping. A name already in use, or an invalid name or service (one over
// identity.MaxServiceLen characters: ErrServiceName), is refused.
func (km *KeyManager) GenerateForOwner(name string, services []string, owner string) (*KeyGenerateResponse, error) {
	m := Machine{ID: identity.NewMachineID(), Name: name, Services: slices.Clone(services)}
	if err := validMachine(m); err != nil {
		return nil, err
	}

	km.mu.Lock()
	defer km.mu.Unlock()
	for _, r := range km.records {
		if r.Name == name {
			return nil, fmt.Errorf("keyfile: a machine named %q exists; revoke it first", name)
		}
	}

	// A cryptographically random key: rpc_ + 48 random bytes (96 hex chars)
	keyBytes := make([]byte, 48)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, fmt.Errorf("generate random key: %w", err)
	}
	apiKey := "rpc_" + hex.EncodeToString(keyBytes)

	km.store.Add(m, apiKey)
	if err := km.appendToFile(m, apiKey); err != nil {
		km.store.Remove(m.ID) // roll back from the live store on a file error
		return nil, fmt.Errorf("persist key: %w", err)
	}

	record := KeyRecord{MachineID: m.ID, Name: m.Name, Services: m.Services, KeyPrefix: apiKey[:12] + "...", Owner: owner, CreatedAt: time.Now().UTC()}
	km.records[m.ID] = record
	km.logger.Info("security: added machine", "machine_id", m.ID, "name", m.Name, "owner", owner, "prefix", record.KeyPrefix)

	return &KeyGenerateResponse{
		MachineID: m.ID,
		Name:      m.Name,
		Services:  m.Services,
		APIKey:    apiKey,
		CreatedAt: record.CreatedAt,
		Message:   "Store this key securely — it will not be shown again.",
	}, nil
}

// Revoke removes machine id's key from both the live store and the on-disk
// key file.
func (km *KeyManager) Revoke(id identity.MachineID) error {
	km.mu.Lock()
	defer km.mu.Unlock()

	if _, exists := km.records[id]; !exists {
		return fmt.Errorf("no machine %q in the key file", id)
	}
	// Remove from the live store and end its connections, whether or not
	// the file below is rewritten.
	km.store.Remove(id)
	if km.onRevoke != nil {
		km.onRevoke(string(id))
	}
	delete(km.records, id)
	if err := km.rewriteFile(); err != nil {
		return fmt.Errorf("persist revocation: %w", err)
	}
	km.logger.Info("security: revoked machine", "machine_id", id)
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

// IsOwnedBy checks whether machine id's key was created by the given email.
func (km *KeyManager) IsOwnedBy(id identity.MachineID, email string) bool {
	km.mu.Lock()
	defer km.mu.Unlock()
	r, exists := km.records[id]
	return exists && r.Owner == email
}

// appendToFile appends machine m's line with key's hash to the key file:
// the plaintext key is never written to disk.
func (km *KeyManager) appendToFile(m Machine, key string) error {
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
	data = append(append(data, m.line(sha256.Sum256([]byte(key)))...), '\n')
	return writeFileAtomic(km.filePath, data)
}

// rewriteFile rebuilds the key file from the current store contents.
// Keys are written hashed: "<machine id> <name> <service,...> sha256:<hex>".
func (km *KeyManager) rewriteFile() error {
	if km.filePath == "" {
		return nil
	}

	km.store.mu.RLock()
	var lines []string
	lines = append(lines, "# API keys managed by the relay (do not edit manually)")
	lines = append(lines, "# Format: <machine id> <name> <service,...> sha256:<hex_hash>")
	for hash, m := range km.store.keys {
		lines = append(lines, m.line(hash))
	}
	km.store.mu.RUnlock()

	return writeFileAtomic(km.filePath, []byte(strings.Join(lines, "\n")+"\n"))
}

// ---------------------------------------------------------------------------
// HTTP Handlers — register on the internal port (not exposed externally)
// ---------------------------------------------------------------------------
