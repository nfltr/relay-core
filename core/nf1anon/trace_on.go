// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package nf1anon

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) writes the
// steps of spec/NF1Anon.tla one source's anonymous registrations and their
// sessions' relayed legs take on this replica (the model's one anonymous
// server and its address budget) to <dir>/nf1anon-<source>-<n>.ndjson,
// each line under the registry's trace lock, inside the step it records
// (the bucket's spend, the leg's end), so the file's order is the order
// they took; spec/TraceNF1Anon.tla checks it (make spec-traces).
type tracer struct {
	mu    sync.Mutex
	files map[netip.Addr]*traceFile
}

type traceFile struct {
	f    *os.File
	legs int
}

var traceN atomic.Int64

func (r *Registry) traceLock() func() {
	r.tr.mu.Lock()
	return r.tr.mu.Unlock
}

// trace writes e for src under traceLock: the id of a new leg (e.Ev
// "Open"), else id.
func (r *Registry) trace(src netip.Addr, e event, id int) int {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" {
		return id
	}
	t := r.tr.files[src]
	if t == nil {
		name := strings.NewReplacer(":", "_", ".", "_").Replace(src.String())
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("nf1anon-%s-%d.ndjson", name, traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		if r.tr.files == nil {
			r.tr.files = map[netip.Addr]*traceFile{}
		}
		t = &traceFile{f: f}
		r.tr.files[src] = t
		// The limits first: the trace's constants (spec/TraceNF1Anon.tla).
		b, _ := json.Marshal(map[string]any{"ev": "Limits", "leg": r.s.LegBytes, "addr": r.s.BucketBytes, "rate": r.s.BucketRate})
		if _, err := f.Write(append(b, '\n')); err != nil {
			panic(err)
		}
	}
	if e.Ev == "Open" {
		t.legs++
		id = t.legs
		e.S = id
	}
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err := t.f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
	return id
}
