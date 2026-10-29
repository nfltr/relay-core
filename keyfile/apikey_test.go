// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAPIKeys_OK(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "keys")
	_ = os.WriteFile(f, []byte("agent-1:secret-a\nagent-2:secret-b\n"), 0644)

	store, err := LoadAPIKeys(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.Count() != 2 {
		t.Fatalf("expected 2 keys, got %d", store.Count())
	}
	agentID, ok := store.Validate("secret-a")
	if !ok || agentID != testAgentID {
		t.Fatalf("expected agent-1, got %q (ok=%v)", agentID, ok)
	}
}

func TestLoadAPIKeys_DuplicateAgentID(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "keys")
	// Same AgentID with two different keys — must be rejected.
	_ = os.WriteFile(f, []byte("agent-1:key-a\nagent-1:key-b\n"), 0644)

	_, err := LoadAPIKeys(f)
	if err == nil {
		t.Fatal("expected error for duplicate AgentID")
	}
	if !strings.Contains(err.Error(), "duplicate AgentID") {
		t.Fatalf("expected 'duplicate AgentID' error, got: %v", err)
	}
}

func TestLoadAPIKeys_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "keys")
	_ = os.WriteFile(f, []byte("# only comments\n"), 0644)

	_, err := LoadAPIKeys(f)
	if err == nil {
		t.Fatal("expected error for empty key file")
	}
}

func TestLoadAPIKeys_SkipsComments(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "keys")
	_ = os.WriteFile(f, []byte("# comment\n\nagent-1:secret\n"), 0644)

	store, err := LoadAPIKeys(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.Count() != 1 {
		t.Fatalf("expected 1 key, got %d", store.Count())
	}
}

func TestValidate_ConstantTime(t *testing.T) {
	store := NewAPIKeyStore()
	store.Add(testAgentID, "correct-key")

	if _, ok := store.Validate("wrong-key"); ok {
		t.Fatal("expected validation to fail for wrong key")
	}
	agentID, ok := store.Validate("correct-key")
	if !ok || agentID != testAgentID {
		t.Fatalf("expected agent-1, got %q", agentID)
	}
}

func TestRemove(t *testing.T) {
	store := NewAPIKeyStore()
	store.Add(testAgentID, "key-a")
	store.Add(testAgentID, "key-b")
	store.Add("agent-2", "key-c")

	store.Remove(testAgentID)
	if store.Count() != 1 {
		t.Fatalf("expected 1 key after remove, got %d", store.Count())
	}
	if _, ok := store.Validate("key-a"); ok {
		t.Fatal("key-a should have been removed")
	}
}

func TestScopedAgentID(t *testing.T) {
	tests := []struct {
		name     string
		keyID    string
		override string
		want     string
	}{
		{"no override", "alice", "", "alice"},
		{"same as key", "alice", "alice", "alice"},
		{"different name", "alice", "my-api", "alice.my-api"},
		{"different name bob", "bob", "my-api", "bob.my-api"},
		{"empty key id with override", "", "my-api", ".my-api"},
		// Already-scoped override: agent sends x-resolved-agent-id back verbatim.
		{"already scoped", "alice", "alice.my-api", "alice.my-api"},
		{"already scoped deep", "alice", "alice.my-api.extra", "alice.my-api.extra"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ScopedAgentID(tc.keyID, tc.override)
			if got != tc.want {
				t.Errorf("ScopedAgentID(%q, %q) = %q, want %q", tc.keyID, tc.override, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// APIKeyStore hot-path benchmarks
// ---------------------------------------------------------------------------

func buildFileStore(b *testing.B, n int) (*APIKeyStore, []string) {
	b.Helper()
	store := NewAPIKeyStore()
	keys := make([]string, n)
	for i := range keys {
		k := "testkey-" + strings.Repeat("x", 80) + string(rune('a'+i%26))
		store.Add("agent-"+string(rune('a'+i%26)), k)
		keys[i] = k
	}
	return store, keys
}

// BenchmarkAPIKeyStore_Validate_Hit measures O(1) map lookup on a hit (55 keys, middle key).
func BenchmarkAPIKeyStore_Validate_Hit(b *testing.B) {
	store, keys := buildFileStore(b, 55)
	target := keys[27]
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := store.Validate(target); !ok {
			b.Fatal("expected hit")
		}
	}
}

// BenchmarkAPIKeyStore_Validate_Hit_Parallel measures concurrent O(1) lookup throughput.
func BenchmarkAPIKeyStore_Validate_Hit_Parallel(b *testing.B) {
	store, keys := buildFileStore(b, 55)
	target := keys[27]
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			store.Validate(target)
		}
	})
}

// BenchmarkAPIKeyStore_Validate_Miss measures O(1) map lookup on a miss.
func BenchmarkAPIKeyStore_Validate_Miss(b *testing.B) {
	store, _ := buildFileStore(b, 55)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		store.Validate("rpc_no_such_key_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	}
}
