// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package nf1cap

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) writes the steps
// of spec/NF1Cap.tla one address takes on this replica to
// <dir>/nf1cap-<rid>-<n>.ndjson, each line under the registry's lock
// inside the step it records, so the file's order is the order they took;
// spec/TraceNF1Cap.tla checks it (make spec-traces).
type tracer struct {
	files map[rid]*os.File
}

var traceN atomic.Int64

// trace writes e for id, under r.mu.
func (r *Registry) trace(id rid, e event) {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" {
		return
	}
	f := r.tr.files[id]
	if f == nil {
		var err error
		f, err = os.OpenFile(filepath.Join(dir, fmt.Sprintf("nf1cap-%s-%d.ndjson", hex.EncodeToString(id[:4]), traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		if r.tr.files == nil {
			r.tr.files = map[rid]*os.File{}
		}
		r.tr.files[id] = f
		// The default cap first: the trace's initial cap (spec/TraceNF1Cap.tla).
		b, _ := json.Marshal(map[string]any{"ev": "Limits", "cap": r.s.Default})
		if _, err := f.Write(append(b, '\n')); err != nil {
			panic(err)
		}
	}
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}
