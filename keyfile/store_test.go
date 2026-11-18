// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
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

func TestLoad(t *testing.T) {
	store, err := Load(writeFile(t, "# comment", "", kl("web", "key-a"), kl("db", "key-b")))
	if err != nil || store.Count() != 2 {
		t.Fatalf("load: %v, %d machines", err, store.Count())
	}
	if m, ok := lookup(store, "key-a"); !ok || m.ID != tm("web") || m.Name != "web" {
		t.Fatalf("line = %+v, %v", m, ok)
	}
	if store, err := Load(writeFile(t, "# only comments")); err != nil || store.Count() != 0 {
		t.Fatalf("a file without lines: %v, %d machines, want an empty store", err, store.Count())
	}
}

// The file refuses what it cannot hold as a machine: a line from before
// machines (agent-id:key, agent-id:sha256:<hex>), an API key line (sha256:,
// gone with the key cut), a machine twice, a name twice in one account, a
// bad id, name or account, a line with a services field (the form before
// any valid service name was allowed).
func TestLoadRefuses(t *testing.T) {
	ok := tmach("web")
	for name, line := range map[string][]string{
		"old plaintext":  {"agent-1:secret"},
		"old hashed":     {"agent-1:sha256:" + strings.Repeat("ab", 32)},
		"API key line":   {strings.Replace(kl("web", "a"), "spki:", "sha256:", 1)},
		"machine twice":  {kl("web", "a"), strings.Replace(kl("db", "b"), string(tm("db")), string(ok.ID), 1)},
		"name twice":     {kl("web", "a"), strings.Replace(kl("db", "b"), " db ", " web ", 1) + " account=" + string(ok.ID)},
		"bad account":    {kl("web", "a") + " account=m-nope"},
		"fifth field":    {kl("web", "a") + " account=" + string(ok.ID) + " x"},
		"bad id":         {strings.Replace(kl("web", "a"), string(ok.ID), "m-nope", 1)},
		"bad name":       {strings.Replace(kl("web", "a"), " web ", " Web ", 1)},
		"services field": {strings.Replace(kl("web", "a"), " web ", " web svc ", 1)},
		"malformed hash": {strings.Replace(kl("web", "a"), "spki:", "spki:zz", 1)},
	} {
		if _, err := Load(writeFile(t, line...)); err == nil {
			t.Errorf("%s: loaded", name)
		}
	}
}

func TestRemove(t *testing.T) {
	store, err := Load(writeFile(t, kl("a", "key-a"), kl("b", "key-c")))
	if err != nil {
		t.Fatal(err)
	}
	store.Remove(tm("a"))
	if _, ok := lookup(store, "key-a"); ok || store.Count() != 1 {
		t.Fatalf("%d machines after remove, a's line kept %v", store.Count(), ok)
	}
}
