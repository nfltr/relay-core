// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/internal/tracehook"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) also writes the
// replica-side steps of spec/Move.tla that the HTTP session of an agent
// that moves takes, to <dir>/move-<agent>-<n>.ndjson, one file per
// session, each line under traceMu (trace_on.go) held across the state
// change it records; spec/TraceMove.tla checks it (make spec-traces).
type traceMove = *traceMoveState

type traceMoveState struct {
	path string // the file, made by the first line
	f    *os.File
	pub  map[string]bool // published, not yet forwarded: the next message of the transaction is its first
	fwd  map[string]bool // forwarded: its last answer is RecvA
	// done: MOVE_DONE went out; ended: the session ended as the model's
	// EndA (the agent's close after MOVE_DONE, the drain's bound);
	// stopped: it ended otherwise (the agent left, a takeover, a relay
	// stop: not Move.tla's), and nothing more is recorded.
	done, ended, stopped bool
}

// moveTrace is s's trace, started at its first step; nil for a session of
// an agent that does not move, or without RELAYTRACE. Under traceMu.
func moveTrace(s *HTTPStream) *traceMoveState {
	if s == nil || s.moved == nil {
		return nil
	}
	if s.mtr == nil {
		dir := os.Getenv("RELAYTRACE")
		if dir == "" {
			return nil
		}
		name := strings.NewReplacer("/", "_", ":", "_").Replace(s.ID)
		s.mtr = &traceMoveState{path: filepath.Join(dir, fmt.Sprintf("move-%s-%d.ndjson", name, traceN.Add(1))), pub: map[string]bool{}, fwd: map[string]bool{}}
	}
	return s.mtr
}

func (t *traceMoveState) log(e moveEvent) {
	if t.stopped {
		return
	}
	switch {
	case e.Ev == "Done":
		t.done = true
	case e.Ev == "DrainBound", e.Ev == "RecvA" && e.M == "EOF":
		t.ended = true
	}
	if t.f == nil {
		f, err := os.OpenFile(t.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
		if err != nil {
			panic(err)
		}
		t.f = f
	}
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err := t.f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}

// traceMoveStep runs change under traceMu and records e when it reports
// the step taken.
func traceMoveStep(s *HTTPStream, e moveEvent, change func() bool) bool {
	traceMu.Lock()
	defer traceMu.Unlock()
	ok := change()
	if t := moveTrace(s); ok && t != nil {
		t.log(e)
	}
	return ok
}

// traceForward sends a message of the topic on the stream; the first of a
// published transaction is Forward.
func traceForward(s *HTTPStream, txn string, send func() error) error {
	traceMu.Lock()
	defer traceMu.Unlock()
	err := send()
	if t := moveTrace(s); err == nil && t != nil && t.pub[txn] {
		delete(t.pub, txn)
		t.fwd[txn] = true
		t.log(moveEvent{Ev: "Forward", R: txn})
	}
	return err
}

// traceAnswer: the last answer of a forwarded transaction is delivered.
func traceAnswer(s *HTTPStream, txn string, deliver func()) {
	traceMu.Lock()
	defer traceMu.Unlock()
	deliver()
	if t := moveTrace(s); t != nil && t.fwd[txn] {
		delete(t.fwd, txn)
		t.log(moveEvent{Ev: "RecvA", M: txn})
	}
}

// sessionOf is the agent's HTTP session here: the one routed to, else the
// one moved away that still ends.
func (d *Dispatcher) sessionOf(agentID string) *HTTPStream {
	if s, ok := d.Agents.Load(agentID); ok {
		return s
	}
	if s, ok := d.movedAway.Load(agentID); ok {
		return s.(*HTTPStream)
	}
	return nil
}

// traceRouteHere is RouteHere recording Pick (ok: the lease taken) and the
// lease's end (Release: after its Publish, or Unlease without one).
func (d *Dispatcher) traceRouteHere(agentID string) (func(), bool) {
	traceMu.Lock()
	defer traceMu.Unlock()
	s := d.sessionOf(agentID)
	_, routed := d.Agents.Load(agentID)
	ok := routed && s.leases.take()
	if t := moveTrace(s); t != nil {
		t.log(moveEvent{Ev: "Pick", OK: ok})
	}
	if !ok {
		return nil, false
	}
	return sync.OnceFunc(func() {
		traceMu.Lock()
		defer traceMu.Unlock()
		s.leases.give()
		if t := moveTrace(s); t != nil {
			t.log(moveEvent{Ev: "Release"})
		}
	}), true
}

// tracePublish publishes the first message of request txn to agentID's
// topic under traceMu and records Publish (ok: on the consumer's channel;
// not: ErrNoConsumer). ponytail: the lock is held across the send, so a
// traced run must not fill a consumer's channel (64 messages): the
// consumer's Forward waits for the lock.
func (d *Dispatcher) tracePublish(agentID, txn string, publish func() error) error {
	traceMu.Lock()
	defer traceMu.Unlock()
	err := publish()
	if t := moveTrace(d.sessionOf(agentID)); t != nil && (err == nil || errors.Is(err, broker.ErrNoConsumer)) {
		t.log(moveEvent{Ev: "Publish", R: txn, OK: err == nil})
		if err == nil {
			t.pub[txn] = true
		}
	}
	return err
}

// traceMoveEOF: the agent closed the stream. After MOVE_DONE that is the
// move's close (RecvA EOF); before it the agent left, which the model does
// not have: the trace ends there.
func traceMoveEOF(s *HTTPStream) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if t := moveTrace(s); t != nil {
		if t.done {
			t.log(moveEvent{Ev: "RecvA", M: "EOF"})
		} else {
			t.stopped = true
		}
	}
}

// traceMoveEnd: the session's watch returned; unless that is the model's
// EndA, the trace ends.
func traceMoveEnd(s *HTTPStream) {
	traceMu.Lock()
	defer traceMu.Unlock()
	if t := moveTrace(s); t != nil && !t.ended {
		t.stopped = true
	}
}

func init() {
	tracehook.Publish = func(d any, agentID, txn string, publish func() error) error {
		if d, ok := d.(*Dispatcher); ok && d != nil {
			return d.tracePublish(agentID, txn, publish)
		}
		return publish()
	}
}
