// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// modules are the modules this module may link outside the standard
// library (README "Dependencies"); a new one is a decision, recorded there
// and in CHANGELOG.md.
var modules = []string{
	"github.com/nfltr/relay-core",
	"github.com/nfltr/link", // and peerlink/quiclink
	"github.com/nfltr/quic-go",
	"github.com/nfltr/relay/client",
	"github.com/nfltr/relay/proto",
	"google.golang.org/grpc",
	"google.golang.org/protobuf",
	"google.golang.org/genproto/googleapis/",
	"github.com/grpc-ecosystem/", // go-grpc-middleware's metrics; grpc-gateway's status types (relay/proto)
	"github.com/prometheus/",
	"github.com/beorn7/perks",
	"github.com/cespare/xxhash/v2",
	"github.com/munnerz/goautoneg",
	"github.com/spf13/", // viper, pflag, cast, afero
	"github.com/go-viper/mapstructure/v2",
	"github.com/pelletier/go-toml/v2",
	"github.com/sagikazarmark/locafero",
	"github.com/sourcegraph/conc",
	"github.com/subosito/gotenv",
	"go.yaml.in/yaml/",
	"gopkg.in/yaml.v3",
	"github.com/fsnotify/fsnotify",
	"github.com/hashicorp/golang-lru/v2",
	"github.com/flynn/noise",
	"golang.org/x/",
}

// TestModuleLinksOnlyItsDependencies: the module's packages (tests aside)
// link the standard library, the modules above and none of a hosted
// product (SQL, Redis, WireGuard, pion); no cgo.
func TestModuleLinksOnlyItsDependencies(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}} {{with .Module}}{{.Path}}{{end}} {{len .CgoFiles}}{{end}}", "./...")
	cmd.Dir = "../.."
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0") // cgo-free: every package builds without it
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for line := range strings.Lines(string(out)) {
		f := strings.Fields(line)
		if len(f) != 3 {
			continue
		}
		pkg, mod, cgo := f[0], f[1], f[2]
		if !slices.ContainsFunc(modules, func(m string) bool { return mod == m || strings.HasPrefix(mod, m) }) {
			t.Errorf("links %s of module %s (not in the allowed modules)", pkg, mod)
		}
		if cgo != "0" {
			t.Errorf("%s has cgo files", pkg)
		}
	}
}

// TestCoreBinaryLinksTheCore: the guard binary links the core, the key
// file and the frontends it attaches.
func TestCoreBinaryLinksTheCore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	deps := strings.Fields(string(out))
	const m = "github.com/nfltr/relay-core/"
	for _, want := range []string{m + "core/session", m + "core/directory", m + "keyfile", m + "compose", m + "frontend/connect", m + "frontend/http", m + "frontend/sni"} {
		if !slices.Contains(deps, want) {
			t.Errorf("does not link %s", want)
		}
	}
}
