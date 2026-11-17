// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package keyfile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) writes the steps
// of spec/KeyAccount.tla one store takes (its lines: Generate, Revoke; its
// codes and certificates: MintCode, Enrol, Reissue, Auth) to
// <dir>/keyaccount-<n>.ndjson. A step that changes the store is written
// under its write lock and an Auth under its read lock, inside the step,
// so the file's order is the order they took; spec/TraceKeyAccount.tla
// checks it (make spec-traces).
type tracer struct {
	mu sync.Mutex
	f  *os.File
}

var traceN atomic.Int64

func (s *APIKeyStore) trace(e event) {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" {
		return
	}
	s.tr.mu.Lock()
	defer s.tr.mu.Unlock()
	if s.tr.f == nil {
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("keyaccount-%d.ndjson", traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		s.tr.f = f
	}
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err := s.tr.f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}

// traceAdd writes m's Generate unless the store holds m already (a second
// key of one machine is no step of the model); s.mu held.
func (s *APIKeyStore) traceAdd(m Machine) {
	for _, x := range s.keys {
		if x.ID == m.ID {
			return
		}
	}
	s.trace(event{Ev: "Generate", M: string(m.ID), N: m.Name, A: string(m.AccountOf())})
}

// traceLoad writes the lines a key file loaded at start holds (Load: the
// state a restart finds; a certificate machine's with its key), each
// account's first machine first.
func (s *APIKeyStore) traceLoad() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, h := range sortedLines(s.keys) {
		m, k := s.keys[h], ""
		if m.Cert {
			k = keyID(h)
		}
		s.trace(event{Ev: "Load", M: string(m.ID), N: m.Name, A: string(m.AccountOf()), K: k})
	}
}

// sortedLines is keys' hashes, each account's first machine before the
// machines that joined it, then by id.
func sortedLines(keys map[[32]byte]Machine) [][32]byte {
	hs := slices.Collect(maps.Keys(keys))
	slices.SortFunc(hs, func(x, y [32]byte) int {
		a, b := keys[x], keys[y]
		ar, br := a.AccountOf() == a.ID, b.AccountOf() == b.ID
		if ar != br {
			if ar {
				return -1
			}
			return 1
		}
		return strings.Compare(string(a.ID), string(b.ID))
	})
	return hs
}

// traceLines writes the lines next has and old lacks (Generate: each
// account's first machine before the machines that joined it) and the
// machines old has and next lacks (Revoke). A line whose key changed in
// the file is neither: the model has no key of a key line.
func (s *APIKeyStore) traceLines(old, next map[[32]byte]Machine) {
	ids := func(keys map[[32]byte]Machine) map[string]Machine {
		out := map[string]Machine{}
		for _, m := range keys {
			out[string(m.ID)] = m
		}
		return out
	}
	o, n := ids(old), ids(next)
	for _, h := range sortedLines(next) {
		if m := next[h]; o[string(m.ID)].ID == "" {
			s.trace(event{Ev: "Generate", M: string(m.ID), N: m.Name, A: string(m.AccountOf())})
		}
	}
	for _, id := range slices.Sorted(func(yield func(string) bool) {
		for id := range o {
			if _, ok := n[id]; !ok && !yield(id) {
				return
			}
		}
	}) {
		s.trace(event{Ev: "Revoke", M: id})
	}
}

// keyID names a key (its SPKI's or token's SHA-256) in the trace.
func keyID(h [32]byte) string { return hex.EncodeToString(h[:6]) }

// codeID names a code in the trace.
func codeID(code string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(code)))
	return hex.EncodeToString(h[:6])
}
