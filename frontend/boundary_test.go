// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package frontend_test

import (
	"os/exec"
	"strings"
	"testing"
)

const (
	module    = "github.com/nfltr/relay-core/"
	frontends = module + "frontend/"
	// composition composes the frontends: no frontend links it.
	composition = module + "compose"
)

// stores are what no frontend links: SQL and its drivers. A frontend
// reaches stored facts through frontend.Host.
var stores = []string{
	"database/sql",
	"modernc.org/sqlite",
	"github.com/jackc/pgx/",
}

// TestFrontendsImportOnlyTheCore: each frontend package links the core and
// shared leaf utilities, never another frontend or the composition
// (core/boundary_test.go keeps the core free of frontends).
func TestFrontendsImportOnlyTheCore(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.ImportPath}} {{join .Deps \" \"}}", "./...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	n := 0
	for line := range strings.Lines(string(out)) {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		pkg, own := f[0], frontendOf(f[0])
		if own == "" {
			continue
		}
		n++
		for _, dep := range f[1:] {
			if strings.HasPrefix(dep, composition) {
				t.Errorf("%s links %s (the composition)", pkg, dep)
			}
			for _, p := range stores {
				if strings.HasPrefix(dep, p) {
					t.Errorf("%s links %s (forbidden: %s)", pkg, dep, p)
				}
			}
			if other := frontendOf(dep); other != "" && other != own {
				t.Errorf("%s links %s (another frontend)", pkg, dep)
			}
		}
	}
	if n == 0 {
		t.Fatal("no frontend package listed")
	}
}

// frontendOf is the frontend a package belongs to ("wireguard" for
// frontend/wireguard and anything under it), "" for none.
func frontendOf(pkg string) string {
	rest, ok := strings.CutPrefix(pkg, frontends)
	if !ok {
		return ""
	}
	name, _, _ := strings.Cut(rest, "/")
	return name
}
