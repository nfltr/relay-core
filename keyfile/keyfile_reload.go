// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"sort"

	"github.com/nfltr/relay-core/internal/platform/filewatch"
)

// Reload reads the key file again into the live store: the keys the file
// now holds replace the store's at once, and the records follow. A subject
// (agent id) whose key left the file, or whose key changed, is revoked:
// OnRevoke runs for it (the relay ends its connections) after its old key
// stopped validating and before a new key of it validates, so the cut never
// takes a connection of the new key. The manager writes the file
// atomically (writeFileAtomic), so a file without keys (empty, or comments
// only) is meant: every key is revoked. A file that does not load
// (missing, unreadable, malformed, a duplicate agent id) changes nothing
// and returns the error. It returns the revoked subjects.
func (km *KeyManager) Reload() ([]string, error) {
	next, err := readAPIKeys(km.filePath)
	if err != nil {
		return nil, err
	}
	km.mu.Lock()
	defer km.mu.Unlock()

	gone := map[string]bool{}
	km.store.mu.RLock()
	for h, id := range km.store.keys {
		if next.keys[h] != id {
			gone[id] = true
		}
	}
	km.store.mu.RUnlock()
	revoked := make([]string, 0, len(gone))
	for id := range gone {
		revoked = append(revoked, id)
	}
	sort.Strings(revoked)

	if len(revoked) > 0 {
		// The revoked subjects' keys, old and new, stop validating first.
		staged := make(map[[32]byte]string, len(next.keys))
		for h, id := range next.keys {
			if !gone[id] {
				staged[h] = id
			}
		}
		km.store.swap(staged)
		if km.onRevoke != nil {
			for _, id := range revoked {
				km.onRevoke(id)
			}
		}
	}
	km.store.swap(next.keys)

	records := make(map[string]KeyRecord, len(next.keys))
	for h, id := range next.keys {
		if r, ok := km.records[id]; ok && !gone[id] {
			records[id] = r
			continue
		}
		records[id] = fileRecord(id, h)
	}
	km.records = records
	km.logger.Info("security: reloaded the API key file", "file", km.filePath, "keys", len(next.keys), "revoked", revoked)
	return revoked, nil
}

// swap replaces the store's keys with keys.
func (s *APIKeyStore) swap(keys map[[32]byte]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = keys
}

// Watch reloads the key file (Reload) whenever it changes, until ctx ends
// (filewatch.Watch). It returns an error only when the file's directory
// cannot be watched.
func (km *KeyManager) Watch(ctx context.Context) error {
	return filewatch.Watch(ctx, km.filePath, func() {
		if _, err := km.Reload(); err != nil {
			km.logger.Warn("security: the API key file changed but does not load: the current keys stay", "file", km.filePath, "error", err)
		}
	}, func(err error) {
		km.logger.Warn("security: API key file watch", "file", km.filePath, "error", err)
	})
}
