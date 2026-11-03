// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package session

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) also writes the
// steps of spec/Presence.tla that the replicas of one in-process test
// deployment take for one agent, to <dir>/presence-<agent>-<n>.ndjson: one
// file for every replica of the group (tracePresenceStart), each line
// under traceMu (trace_on.go) inside the lock that orders its change (the
// feed's Mu, the stream's record lock), so the file's order is an order
// the replicas took; spec/TracePresence.tla checks it (make spec-traces).
type tracePres = *presTraceFeed

// presTraceGroup is one deployment's trace: its replicas' feeds write to f.
type presTraceGroup struct {
	f     *os.File
	agent string
}

// presTraceFeed names a feed's replica in its group.
type presTraceFeed struct {
	g    *presTraceGroup
	name string
}

// presEvent is one line: ev the step, r the replica, ok a record write
// that landed, lab the labels (Connect, Update, Snap), on a listing that
// has the agent (Snap).
type presEvent struct {
	Ev  string `json:"ev"`
	R   string `json:"r"`
	OK  bool   `json:"ok"`
	Lab string `json:"lab"`
	On  bool   `json:"on"`
}

// tracePresenceStart starts the trace of agent's presence across the
// replicas joined to it; nil without RELAYTRACE.
func tracePresenceStart(agent string) *presTraceGroup {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" {
		return nil
	}
	name := strings.NewReplacer("/", "_", ":", "_").Replace(agent)
	f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("presence-%s-%d.ndjson", name, traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
	if err != nil {
		panic(err)
	}
	return &presTraceGroup{f: f, agent: agent}
}

// tracePresenceJoin makes f replica name of g.
func tracePresenceJoin(f *presenceFeed, g *presTraceGroup, name string) {
	if g != nil {
		f.Mu.Lock()
		f.tr = &presTraceFeed{g: g, name: name}
		f.Mu.Unlock()
	}
}

// presLabels is a label set as one string (the trace module tells the
// connect labels from an update's by it).
func presLabels(l map[string]string) string {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(l)) {
		fmt.Fprintf(&b, "%s=%s,", k, l[k])
	}
	return b.String()
}

// log writes e for t's replica; under traceMu.
func (t *presTraceFeed) log(e presEvent) {
	if t == nil {
		return
	}
	e.R = t.name
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err := t.g.f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}

func (t *presTraceFeed) step(e presEvent) {
	if t == nil {
		return
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	t.log(e)
}

// tracePresBump: the feed took a change of the agent (under f.Mu): a local
// one is Publish, a peer's Deliver.
func tracePresBump(f *presenceFeed, ev *presencev1.PresenceEvent, origin string) {
	if f.tr == nil || ev.GetAgentId() != f.tr.g.agent {
		return
	}
	e := presEvent{Ev: "Publish"}
	if origin == "peer" {
		e.Ev = "Deliver"
	}
	f.tr.step(e)
}

// tracePresResync: the bus's resync (under f.Mu): Presence.tla's
// Resubscribe after its Drop.
func tracePresResync(f *presenceFeed) { f.tr.step(presEvent{Ev: "Resync"}) }

// tracePresWake: Watch answered the agent's events or reset (under f.Mu).
func tracePresWake(f *presenceFeed, events []*presencev1.PresenceEvent, reset bool) {
	if f.tr == nil {
		return
	}
	if reset || slices.ContainsFunc(events, func(ev *presencev1.PresenceEvent) bool { return ev.GetAgentId() == f.tr.g.agent }) {
		f.tr.step(presEvent{Ev: "Wake"})
	}
}

// tracePresCursor: the watcher's re-list took the cursor (under f.Mu).
func tracePresCursor(f *presenceFeed) { f.tr.step(presEvent{Ev: "Cursor"}) }

// tracePresSnap: the watcher's re-list answered the agent (on, its labels)
// or not. The watcher's listing is relay-server's (myAgents): its tests
// record it.
func tracePresSnap(f *presenceFeed, on bool, labels map[string]string) {
	f.Mu.Lock()
	t := f.tr
	f.Mu.Unlock()
	t.step(presEvent{Ev: "Snap", On: on, Lab: presLabels(labels)})
}

// tracePresRecord: a step of the agent's session on d (Connect, Renew,
// Update, Superseded, End, LateDown), ok the record write landed.
func tracePresRecord(d *Dispatcher, agentID, ev string, ok bool, labels map[string]string) {
	d.Presence.Mu.Lock()
	t := d.Presence.tr
	d.Presence.Mu.Unlock()
	if t == nil || agentID != t.g.agent {
		return
	}
	t.step(presEvent{Ev: ev, OK: ok, Lab: presLabels(labels)})
}
