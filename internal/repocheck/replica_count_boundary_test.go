// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package repocheck

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot is this module's root.
const repoRoot = "../.."

// replicaCountAllow are today's replica-count branches (relay-server
// docs/decisions/one-replica-same-path-2026-11-27.md section 1, "special"
// rows): the path and the trimmed line, with the lane that deletes it.
// relay-server's are in its own TestNoReplicaCountBranch.
var replicaCountAllow = []struct{ site, lane string }{
	{"core/session/agent_move.go: return d.PodDialer != nil && d.globalContext != nil && d.globalContext.Err() != nil", "MR-N1-CORE (1e: one drain)"},
	{"core/session/agent_move.go: return s, ok && store && s.moved != nil && d.PodDialer != nil", "MR-N1-CORE (1e)"},
	{"core/session/agent_rebalance.go: if d.PodDialer == nil || replicas < 2 || d.Leaving() {", "MR-N1-CORE (1e: drop the nil half)"},
	{"core/session/core_run.go: if d.PodDialer != nil {", "MR-N1-CORE (1e: one drain)"},
	{"core/session/core_run.go: if c.Dispatcher != nil && c.Dispatcher.PodDialer != nil {", "MR-N1-CORE (1e)"},
	{`core/session/dispatcher.go: if config.IsK8sMultiPodEnv(viper.GetString("ENV")) {`, "MR-N1-CORE (1a: always a PodDialer)"},
	{"core/session/dispatcher_grpc.go: if d.PodDialer == nil {", "MR-N1-CORE (1d: one not-found answer)"},
	{"core/session/dispatcher_worker_liveness.go: if d.Owner == nil {", "MR-N1-CORE (1c: Owner always set)"},
	{"core/session/presence.go: if bus == nil {", "MR-N1-CORE (1f: BindBus never nil)"},
	{"frontend/http/frontend.go: if d := c.Dispatcher; d != nil && d.PodDialer != nil {", "MR-N1-CORE (1d: the peer dial always wrapped)"},
	{"platform/config/config.go: func IsLocalEnv(env string) bool {", "MR-N1-CORE then MR-N1-SERVER (1a: no ENV in replica decisions)"},
	{"platform/config/config.go: func IsK8sMultiPodEnv(env string) bool {", "MR-N1-CORE (1a: deleted)"},
}

// replicaCountBranch matches a decision on the replica count: the ENV
// switch (IsK8sMultiPodEnv, IsLocalEnv, the Env constants, a read or
// setting of ENV) or a nil PodDialer, Owner or presence/revocation bus.
// relay-server and relay-core hold the same test; each lists its own sites.
var replicaCountBranch = regexp.MustCompile(`IsK8sMultiPodEnv\(|IsLocalEnv\(|\bconfig\.Env(K8s|CI|Standalone|Local)\b|(?i:GetString\("env"\))|"ENV[":=]|\bPodDialer [!=]= nil|\bd\.Owner == nil|\b(bus|PresenceBus|revocationBus|orgCRLBus) [!=]= nil`)

// TestNoReplicaCountBranch: one code path for every replica count
// (relay-server docs/decisions/one-replica-same-path-2026-11-27.md; the
// standalone VM is a set of one). A Go file outside tests, vendor/ and
// testdata/ holds no line replicaCountBranch matches but those of
// replicaCountAllow, and every allowed line still exists: MR-N1-CORE and
// MR-N1-SERVER delete their rows until the list is empty.
func TestNoReplicaCountBranch(t *testing.T) {
	left := map[string]int{}
	for _, a := range replicaCountAllow {
		left[a.site]++
	}
	err := filepath.WalkDir(repoRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); n == "vendor" || n == "testdata" || n == "node_modules" || (n != "." && n != ".." && strings.HasPrefix(n, ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(p) // #nosec G304 -- this repository's own files
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(repoRoot, p)
		for i, line := range strings.Split(string(src), "\n") {
			if !replicaCountBranch.MatchString(line) {
				continue
			}
			site := filepath.ToSlash(rel) + ": " + strings.TrimSpace(line)
			if left[site] == 0 {
				t.Errorf("%s:%d: a replica-count branch: %s\n\tone path for every count (docs/decisions/one-replica-same-path-2026-11-27.md)", rel, i+1, strings.TrimSpace(line))
				continue
			}
			left[site]--
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for site, n := range left {
		if n > 0 {
			t.Errorf("stale replicaCountAllow entry (the line is gone: delete it): %s", site)
		}
	}
}
