// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package session

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) also writes the
// relay's steps of spec/Takeover.tla for each agent ID of a dispatcher,
// to <dir>/takeover-<agent>-<n>.ndjson: each ConnectHTTP is a process
// (p1, p2, ...); its takeover decision (Connect, or Refuse for another
// credential, naming the session it replaces or is refused for) and its
// end (Notice when it was replaced, else End) are each one line written
// under takeoverMu together with the change it records (the load of the
// live session; the session's removal), so the file's order is the
// order of those changes. spec/TraceTakeover.tla checks it (make
// spec-traces). takeoverMu is its own lock: the removal publishes
// presence, whose recorder takes traceMu.
var (
	takeoverMu    sync.Mutex
	takeoverFiles = map[takeoverKey]*takeoverFile{} // under takeoverMu
)

type takeoverKey struct {
	d     *Dispatcher
	agent string
}

type takeoverFile struct {
	f *os.File
	n int // processes named
}

// takeoverEvent is one line: ev the step, p the process, c its
// credential (a prefix of its hash), q the session it replaces or is
// refused for ("" none).
type takeoverEvent struct {
	Ev string `json:"ev"`
	P  string `json:"p"`
	C  string `json:"c"`
	Q  string `json:"q"`
}

// traceTakeoverProc names a new ConnectHTTP of agentID on d; "" without
// RELAYTRACE.
func traceTakeoverProc(d *Dispatcher, agentID string) string {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" {
		return ""
	}
	takeoverMu.Lock()
	defer takeoverMu.Unlock()
	k := takeoverKey{d, agentID}
	t := takeoverFiles[k]
	if t == nil {
		name := strings.NewReplacer("/", "_", ":", "_").Replace(agentID)
		f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("takeover-%s-%d.ndjson", name, traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		t = &takeoverFile{f: f}
		takeoverFiles[k] = t
	}
	t.n++
	return fmt.Sprintf("p%d", t.n)
}

// traceTakeoverStep runs step, the change it records, and writes its line
// (ev, q) for process proc, as one step under takeoverMu.
func traceTakeoverStep(d *Dispatcher, agentID, proc, credHash string, step func() (ev, q string)) {
	if proc == "" {
		step()
		return
	}
	takeoverMu.Lock()
	defer takeoverMu.Unlock()
	ev, q := step()
	b, err := json.Marshal(takeoverEvent{Ev: ev, P: proc, C: credHash[:min(8, len(credHash))], Q: q})
	if err != nil {
		panic(err)
	}
	if _, err := takeoverFiles[takeoverKey{d, agentID}].f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}
