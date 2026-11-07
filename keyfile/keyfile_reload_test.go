// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// writeKeys writes a key file; a "name:key" line is test machine name's
// line with that key (kl), any other line is written as it is.
func writeKeys(t *testing.T, path string, lines ...string) {
	t.Helper()
	data := ""
	for _, l := range lines {
		if name, key, ok := strings.Cut(l, ":"); ok && !strings.HasPrefix(l, "#") && !strings.Contains(l, " ") {
			l = kl(name, key)
		}
		data += l + "\n"
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

// reloadingManager is a key manager over a key file holding lines, with
// the subjects OnRevoke was called with.
func reloadingManager(t *testing.T, lines ...string) (km *KeyManager, store *APIKeyStore, path string, revoked func() []string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "api-keys")
	writeKeys(t, path, lines...)
	store, err := LoadAPIKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	km = NewKeyManager(path, store, testKeyManagerLogger())
	var mu sync.Mutex
	var got []string
	km.OnRevoke(func(id string) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, id)
	})
	return km, store, path, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(got)
	}
}

func TestKeyManagerReload(t *testing.T) {
	km, store, path, revoked := reloadingManager(t, "a:key-a", "b:key-b", "c:key-c")
	// b removed, c re-keyed, d added.
	writeKeys(t, path, "# comment", "a:key-a", "c:key-c2", "d:key-d")
	got, err := km.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if want := sortedIDs("b", "c"); !slices.Equal(machineStrings(got), want) || !slices.Equal(revoked(), want) {
		t.Fatalf("revoked %v, OnRevoke %v, want %v", got, revoked(), want)
	}
	for key, want := range map[string]string{"key-a": "a", "key-c2": "c", "key-d": "d", "key-b": "", "key-c": ""} {
		if m, _ := store.Validate(key); m.Name != want {
			t.Errorf("Validate(%s) = %q, want %q", key, m.Name, want)
		}
	}
	var names []string
	for _, r := range km.List() {
		names = append(names, r.Name)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"a", "c", "d"}) {
		t.Fatalf("records %v, want a c d", names)
	}
	// The same file again revokes nothing.
	if got, err := km.Reload(); err != nil || len(got) != 0 {
		t.Fatalf("second reload: %v %v", got, err)
	}
}

// A file without keys is meant (the manager writes it atomically): every
// key is revoked, the last one included.
func TestKeyManagerReloadOfNoKeysRevokesAll(t *testing.T) {
	for name, lines := range map[string][]string{"empty": nil, "comments": {"# only a header"}} {
		t.Run(name, func(t *testing.T) {
			km, store, path, revoked := reloadingManager(t, "a:key-a", "b:key-b")
			writeKeys(t, path, lines...)
			if _, err := km.Reload(); err != nil {
				t.Fatal(err)
			}
			if got := revoked(); !slices.Equal(got, sortedIDs("a", "b")) || store.Count() != 0 || len(km.List()) != 0 {
				t.Fatalf("revoked %v, %d keys, %d records; want a b revoked, none left", got, store.Count(), len(km.List()))
			}
		})
	}
}

// A file that does not load keeps the keys.
func TestKeyManagerReloadKeepsKeysOnBadFile(t *testing.T) {
	for name, write := range map[string]func(path string){
		"malformed": func(p string) { writeKeys(t, p, string(tm("a"))+" a sha256:nothex") },
		"duplicate": func(p string) { writeKeys(t, p, "a:key-a", "a:key-x") },
		"missing":   func(p string) { _ = os.Remove(p) },
	} {
		t.Run(name, func(t *testing.T) {
			km, store, path, revoked := reloadingManager(t, "a:key-a", "b:key-b")
			write(path)
			if _, err := km.Reload(); err == nil {
				t.Fatal("Reload of a bad file: no error")
			}
			if len(revoked()) != 0 || store.Count() != 2 || len(km.List()) != 2 {
				t.Fatalf("revoked %v, %d keys, %d records; want none revoked, 2 kept", revoked(), store.Count(), len(km.List()))
			}
			if m, ok := store.Validate("key-b"); !ok || m.Name != "b" {
				t.Fatal("key-b no longer validates")
			}
		})
	}
}

// A subject's new key does not validate while its old key's connections
// are being cut: OnRevoke sees neither key.
func TestKeyManagerReloadRekeyCutsBeforeNewKey(t *testing.T) {
	km, store, path, _ := reloadingManager(t, "a:key-a")
	var during []bool
	km.OnRevoke(func(string) {
		_, oldOK := store.Validate("key-a")
		_, newOK := store.Validate("key-a2")
		during = []bool{oldOK, newOK}
	})
	writeKeys(t, path, "a:key-a2")
	if _, err := km.Reload(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(during, []bool{false, false}) {
		t.Fatalf("during OnRevoke old/new key valid = %v, want neither", during)
	}
	if m, _ := store.Validate("key-a2"); m.Name != "a" {
		t.Fatal("the new key does not validate after the reload")
	}
}

// Watch reloads after the file is rewritten (as `keys revoke` does) and
// after it is replaced by a rename (as editors do).
func TestKeyManagerWatch(t *testing.T) {
	km, store, path, revoked := reloadingManager(t, "a:key-a", "b:key-b", "c:key-c")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- km.Watch(ctx) }()
	time.Sleep(100 * time.Millisecond) // the watch is set up

	await := func(want []string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !slices.Equal(revoked(), want) {
			if time.Now().After(deadline) {
				t.Fatalf("revoked %v, want %v", revoked(), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	writeKeys(t, path, "a:key-a", "c:key-c")
	await(ids("b"))
	tmp := path + ".tmp"
	writeKeys(t, tmp, "c:key-c")
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
	await(ids("b", "a"))
	if store.Count() != 1 {
		t.Fatalf("%d keys, want 1", store.Count())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// The manager replaces the key file whole (rename over it), keeping its
// mode, and leaves no temporary file; revoking the last key leaves a file
// without keys that a reload reads as such.
func TestKeyManagerWritesKeyFileAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "api-keys")
	writeKeys(t, path, "# header")
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	km := NewKeyManager(path, nil, testKeyManagerLogger())
	before, _ := os.Stat(path)
	resp, err := km.Generate("a")
	if err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if os.SameFile(before, after) {
		t.Fatal("Generate appended in place, want the file replaced")
	}
	if after.Mode().Perm() != 0o640 {
		t.Fatalf("mode %v, want 0640 kept", after.Mode().Perm())
	}
	data, _ := os.ReadFile(path)
	if !strings.HasPrefix(string(data), "# header\n"+string(resp.MachineID)+" a sha256:") {
		t.Fatalf("file %q, want the header then the new key", data)
	}
	if err := km.Revoke(resp.MachineID); err != nil {
		t.Fatal(err)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 1 {
		t.Fatalf("%d entries in the directory, want only the key file", len(entries))
	}
	if store, err := readAPIKeys(path); err != nil || store.Count() != 0 {
		t.Fatalf("after revoking the last key: %v keys, %v", store, err)
	}
}

// A rename in the file cuts nothing (names are labels: spec RenameKeepsReach).
func TestKeyManagerReloadRenameCutsNothing(t *testing.T) {
	km, store, path, revoked := reloadingManager(t, "a:key-a", "b:key-b")
	renamed := strings.Replace(kl("a", "key-a"), " a ", " a2 ", 1)
	writeKeys(t, path, renamed, kl("b", "key-b"))
	if _, err := km.Reload(); err != nil {
		t.Fatal(err)
	}
	if got := revoked(); len(got) != 0 {
		t.Fatalf("revoked %v, want none (a rename)", got)
	}
	if m, ok := store.Validate("key-a"); !ok || m.ID != tm("a") || m.Name != "a2" {
		t.Fatalf("renamed machine: %+v %v", m, ok)
	}
}

func machineStrings(ms []identity.MachineID) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = string(m)
	}
	return out
}
