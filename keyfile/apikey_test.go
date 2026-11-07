// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, lines ...string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "keys")
	if err := os.WriteFile(f, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestLoadAPIKeys(t *testing.T) {
	store, err := LoadAPIKeys(writeFile(t, "# comment", "", kl("web", "secret-a"), kl("db", "secret-b")))
	if err != nil || store.Count() != 2 {
		t.Fatalf("load: %v, %d keys", err, store.Count())
	}
	if m, ok := store.Validate("secret-a"); !ok || m.ID != tm("web") || m.Name != "web" || len(m.Services) != 1 {
		t.Fatalf("Validate = %+v, %v", m, ok)
	}
	if _, ok := store.Validate("wrong-key"); ok {
		t.Fatal("a wrong key validated")
	}
	if _, err := LoadAPIKeys(writeFile(t, "# only comments")); err == nil {
		t.Fatal("a file without keys loaded")
	}
}

// The file refuses what it cannot hold as a machine: a line from before
// machines (agent-id:key, agent-id:sha256:<hex>), a machine or a name
// twice, a bad id, name or service (a 35-character service included).
func TestLoadAPIKeysRefuses(t *testing.T) {
	ok := tmach("web")
	for name, line := range map[string][]string{
		"old plaintext":  {"agent-1:secret"},
		"old hashed":     {"agent-1:sha256:" + strings.Repeat("ab", 32)},
		"machine twice":  {kl("web", "a"), strings.Replace(kl("db", "b"), string(tm("db")), string(ok.ID), 1)},
		"name twice":     {kl("web", "a"), strings.Replace(kl("db", "b"), " db ", " web ", 1)},
		"bad id":         {strings.Replace(kl("web", "a"), string(ok.ID), "m-nope", 1)},
		"bad name":       {strings.Replace(kl("web", "a"), " web ", " Web ", 1)},
		"long service":   {strings.Replace(kl("web", "a"), " svc ", " "+strings.Repeat("s", 35)+" ", 1)},
		"service twice":  {strings.Replace(kl("web", "a"), " svc ", " svc,svc ", 1)},
		"malformed hash": {strings.Replace(kl("web", "a"), "sha256:", "sha256:zz", 1)},
	} {
		if _, err := LoadAPIKeys(writeFile(t, line...)); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}

func TestLoadAPIKeysFromEnv(t *testing.T) {
	t.Setenv("KF_TEST", string(tm("web"))+" web ssh,http secret")
	store, err := LoadAPIKeysFromEnv("KF_TEST")
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := store.Validate("secret"); !ok || m.ID != tm("web") || strings.Join(m.Services, ",") != "ssh,http" {
		t.Fatalf("Validate = %+v, %v", m, ok)
	}
	t.Setenv("KF_TEST", "web:secret")
	if _, err := LoadAPIKeysFromEnv("KF_TEST"); err == nil {
		t.Fatal("an agent-id:key value loaded")
	}
	t.Setenv("KF_TEST", string(tm("web"))+" web "+strings.Repeat("s", 35)+" secret")
	if _, err := LoadAPIKeysFromEnv("KF_TEST"); !errors.Is(err, ErrServiceName) {
		t.Fatalf("a 35-character service: %v, want ErrServiceName", err)
	}
}

func TestRemove(t *testing.T) {
	store := NewAPIKeyStore()
	store.Add(tmach("a"), "key-a")
	store.Add(tmach("a"), "key-b")
	store.Add(tmach("b"), "key-c")
	store.Remove(tm("a"))
	if _, ok := store.Validate("key-a"); ok || store.Count() != 1 {
		t.Fatalf("%d keys after remove, key-a valid %v", store.Count(), ok)
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
		store.Add(tmach("agent-"+string(rune('a'+i%26))), k)
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
