// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"

	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// KeyRecord holds metadata about a machine of the file.
type KeyRecord struct {
	MachineID identity.MachineID `json:"machine_id"`
	Name      string             `json:"name"`
	Account   identity.MachineID `json:"account"`              // its account (Machine.AccountOf)
	KeyPrefix string             `json:"key_prefix"`           // its key's SHA-256 prefix, for identification
	CreatedAt time.Time          `json:"created_at,omitempty"` // its enrolment (zero: read from the file)
}

// KeyListResponse is returned when listing keys.
type KeyListResponse struct {
	Keys []KeyRecord `json:"keys"`
}

// KeyManager adds, persists and revokes the key file's machines. It
// manages both the in-memory Store and the on-disk key file.
type KeyManager struct {
	mu       sync.Mutex
	store    *Store
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
func NewKeyManager(filePath string, store *Store, logger *slog.Logger) *KeyManager {
	if store == nil {
		store = NewStore()
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

// fileRecord is the record of a line read from the file: its hash's prefix.
func fileRecord(m Machine, hash [32]byte) KeyRecord {
	return KeyRecord{MachineID: m.ID, Name: m.Name, Account: m.AccountOf(), KeyPrefix: hex.EncodeToString(hash[:4]) + "..."}
}

// accountOfMember is the account of the machine member names (its id, or
// a name only one machine of the file holds); km.mu held. A machine the
// file does not hold is refused (spec/KeyAccount.tla AccountRooted).
func (km *KeyManager) accountOfMember(member string) (identity.MachineID, error) {
	if r, ok := km.records[identity.MachineID(member)]; ok {
		return r.Account, nil
	}
	var found []KeyRecord
	for _, r := range km.records {
		if r.Name == member {
			found = append(found, r)
		}
	}
	switch len(found) {
	case 0:
		return "", fmt.Errorf("keyfile: --account %q: no such machine in the key file", member)
	case 1:
		return found[0].Account, nil
	}
	return "", fmt.Errorf("keyfile: --account %q: %d machines have that name; give its machine id", member, len(found))
}

// Revoke removes machine id's line from both the live store and the on-disk
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

// List returns the file's machines.
func (km *KeyManager) List() []KeyRecord {
	km.mu.Lock()
	defer km.mu.Unlock()

	records := make([]KeyRecord, 0, len(km.records))
	for _, r := range km.records {
		records = append(records, r)
	}
	return records
}

// appendLine appends machine m's line for hash h to the key file.
func (km *KeyManager) appendLine(m Machine, h [32]byte) error {
	if km.filePath == "" {
		return nil // no file persistence (a test's store)
	}
	data, err := os.ReadFile(km.filePath) // #nosec G304 -- operator-configured key file
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(data) > 0 && data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	data = append(append(data, m.line(h)...), '\n')
	return writeFileAtomic(km.filePath, data)
}

// rewriteFile rebuilds the key file from the current store contents:
// "<machine id> <name> spki:<hex> [account=<machine id>]".
func (km *KeyManager) rewriteFile() error {
	if km.filePath == "" {
		return nil
	}

	km.store.mu.RLock()
	var lines []string
	lines = append(lines, "# Machines managed by the relay (do not edit manually)")
	lines = append(lines, "# Format: <machine id> <name> spki:<hex_hash> [account=<machine id>]")
	for hash, m := range km.store.keys {
		lines = append(lines, m.line(hash))
	}
	km.store.mu.RUnlock()

	return writeFileAtomic(km.filePath, []byte(strings.Join(lines, "\n")+"\n"))
}
