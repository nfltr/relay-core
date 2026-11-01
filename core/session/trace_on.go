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
	"sync/atomic"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"

	"github.com/nfltr/relay-core/platform/metrics"
)

// Trace mode (go test -tags relaytrace, RELAYTRACE=<dir>) writes one JSON
// line per relay-side step of spec/AgentLink.tla that a gRPC frame stream
// with per-session credit takes, to <dir>/<agent>-<n>.ndjson, one file per
// stream. Each line is written under one process-wide lock held across the
// state change it records (a receive's processing, a frame's insertion
// into the outbox, a session's removal), so a file's order is an order the
// steps took; spec/Trace.tla checks it against the model (make
// spec-traces). After the stream's Down nothing more is recorded.
const traceOn = true

type (
	traceStream = *traceStreamState
	traceSess   = *traceSessState
)

type traceStreamState struct {
	f       *os.File
	stopped bool // Down was recorded (under traceMu)
}

type traceSessState struct {
	t   *traceStreamState
	sid string
}

var (
	traceMu sync.Mutex
	traceN  atomic.Int64
)

// traceEvent is one line: ev the model step, s the session, n its size
// (bytes, a window update; Up: the relay's window), t a received frame's
// type (data, win, half, whole) or a sent close's (half, whole).
type traceEvent struct {
	Ev string `json:"ev"`
	S  string `json:"s"`
	N  int    `json:"n"`
	T  string `json:"t"`
	W  int    `json:"w,omitempty"` // Up: the agent's window

}

func traced(a *tcpAgentStream) bool { return a.tr != nil }

// traceUp starts the trace of a gRPC stream with credit when RELAYTRACE
// names a directory.
func traceUp(a *tcpAgentStream) {
	dir := os.Getenv("RELAYTRACE")
	if dir == "" || a.cr == nil || a.DC != nil {
		return
	}
	name := strings.NewReplacer("/", "_", ":", "_").Replace(a.agent)
	f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%s-%d.ndjson", name, traceN.Add(1))), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // #nosec G304 -- the tester's RELAYTRACE
	if err != nil {
		panic(err)
	}
	a.tr = &traceStreamState{f: f}
	traceMu.Lock()
	defer traceMu.Unlock()
	traceLog(a.tr, traceEvent{Ev: "Up", N: a.cr.Window(), W: a.cr.PeerWindow()})
}

// traceLog writes e; under traceMu.
func traceLog(t *traceStreamState, e traceEvent) {
	if t == nil || t.stopped || e.Ev == "" { // a control frame: no step
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		panic(err)
	}
	if _, err := t.f.Write(append(b, '\n')); err != nil {
		panic(err)
	}
}

func traceStep(t *traceStreamState, e traceEvent) {
	if t == nil {
		return
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	traceLog(t, e)
}

// traceOpenSess names a session for its pops (Read).
func traceOpenSess(a *tcpAgentStream, id string, s *tcpSession) {
	if a.tr != nil {
		s.tr = &traceSessState{t: a.tr, sid: id}
	}
}

// traceWrite: sendData's Credit.Acquire took n.
func traceWrite(a *tcpAgentStream, id string, n int) {
	traceStep(a.tr, traceEvent{Ev: "Write", S: id, N: n})
}

// traceRead: n bytes popped from the session's queue (Credit.Consumed
// follows).
func traceRead(s *tcpSession, n int) {
	if s.tr != nil {
		traceStep(s.tr.t, traceEvent{Ev: "Read", S: s.tr.sid, N: n})
	}
}

// traceDown: the stream ended here (unregister): CloseAll follows.
func traceDown(a *tcpAgentStream) {
	if a.tr == nil {
		return
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	traceLog(a.tr, traceEvent{Ev: "Down"})
	a.tr.stopped = true
}

// traceFinish: the session is removed (removeSession); remove runs under
// traceMu.
func traceFinish(a *tcpAgentStream, id string, remove func()) {
	traceMu.Lock()
	defer traceMu.Unlock()
	remove()
	traceLog(a.tr, traceEvent{Ev: "Finish", S: id})
}

// traceEnqueue is enqueue for a traced gRPC stream: the frame goes into the
// outbox and its line is written under traceMu, so the file's order of the
// stream's frames is the wire's. Every sender of the stream comes here, and
// only the sender loop takes frames out, so a free slot stays free under
// the lock; a full outbox is waited on outside it.
func traceEnqueue(a *tcpAgentStream, f *pb.TCPData) {
	for {
		traceMu.Lock()
		if len(a.SendCh) < cap(a.SendCh) {
			a.SendCh <- f
			metrics.AddTransferBytes(metrics.PathTCPGRPC, len(f.GetData()), 0)
			traceLog(a.tr, sentEvent(f))
			traceMu.Unlock()
			return
		}
		traceMu.Unlock()
		select {
		case <-a.Done: // the stream ended: the frame is lost (SendData on a dead stream)
			traceStep(a.tr, sentEvent(f))
			return
		case <-time.After(50 * time.Microsecond):
		}
	}
}

// sentEvent is the step a frame the relay sends stands for.
func sentEvent(f *pb.TCPData) traceEvent {
	sid := f.GetSessionId()
	switch {
	case sid == dataplane.WindowSID:
		s, n, err := dataplane.ParseWindowFrame(f)
		if err != nil {
			panic(err)
		}
		return traceEvent{Ev: "SendWin", S: s, N: n}
	case strings.HasPrefix(sid, "_"):
		return traceEvent{} // control: not a step of the model
	case f.GetOpen():
		return traceEvent{Ev: "Open", S: sid}
	case f.GetClose() && f.GetHalf():
		return traceEvent{Ev: "CloseWrite", S: sid, T: "half"}
	case f.GetClose():
		return traceEvent{Ev: "Abort", S: sid, T: "whole"}
	case len(f.GetData()) > 0:
		return traceEvent{Ev: "SendData", S: sid, N: len(f.GetData())}
	}
	return traceEvent{}
}

// traceRecv records a frame the relay received and runs its processing
// (process) under traceMu.
func traceRecv(a *tcpAgentStream, f *pb.TCPData, process func() error) error {
	traceMu.Lock()
	defer traceMu.Unlock()
	e := traceEvent{Ev: "Recv", S: f.GetSessionId()}
	switch sid := f.GetSessionId(); {
	case sid == dataplane.WindowSID:
		s, n, err := dataplane.ParseWindowFrame(f)
		if err == nil {
			e.T, e.S, e.N = "win", s, n
		}
	case strings.HasPrefix(sid, "_"):
	case f.GetClose() && f.GetHalf():
		e.T = "half"
	case f.GetClose():
		e.T = "whole"
	case len(f.GetData()) > 0:
		e.T, e.N = "data", len(f.GetData())
	}
	if e.T != "" {
		traceLog(a.tr, e)
	}
	return process()
}
