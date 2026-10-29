// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package core_test

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// forbidden are what the core links never: a frontend, the composition,
// SQL, the Redis client, OPA, WireGuard. One replica's directory is in
// process; several replicas share a directory implemented outside this
// module (directory.Cache).
var forbidden = []string{
	"github.com/nfltr/relay-core/frontend/",
	"github.com/nfltr/relay-core/compose",
	"database/sql",
	"github.com/redis/go-redis/",
	"github.com/jackc/pgx/",
	"modernc.org/sqlite",
	"github.com/open-policy-agent/",
	// WireGuard is a frontend of the hosted relay: the core carries its
	// frames through a session hook and speaks none of its protocol.
	"github.com/nfltr/relay/proto/wire/wgwire",
	"golang.zx2c4.com/wireguard",
}

// TestCoreLinksNoFrontendOrStore: go list -deps ./core/... names none of
// the forbidden packages, and only gRPC's transport links
// net/http/httputil (the core imports it nowhere).
func TestCoreLinksNoFrontendOrStore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}} {{join .Imports \" \"}}", "./...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, out)
	}
	for line := range strings.Lines(string(out)) {
		f := strings.Fields(line)
		if len(f) == 0 {
			continue
		}
		pkg := f[0]
		if i := slices.IndexFunc(forbidden, func(p string) bool { return strings.HasPrefix(pkg, p) }); i >= 0 {
			t.Errorf("the core links %s (forbidden: %s)", pkg, forbidden[i])
		}
		if slices.Contains(f[1:], "net/http/httputil") && pkg != "google.golang.org/grpc/internal/transport" {
			t.Errorf("%s imports net/http/httputil", pkg)
		}
	}
}
