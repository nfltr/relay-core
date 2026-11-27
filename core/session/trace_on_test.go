// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package session

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nfltr/relay/proto/pb"
)

// A chunk whose Acquire returned before its session's Finish but reached
// the recorder after it is left out with its SendData: the trace keeps
// the order the steps took (spec/README.md "Trace validation"); a Write
// logged before the Finish keeps its SendData after it.
func TestTraceWriteAfterFinishIsLeftOut(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "t.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	a := NewTCPAgentStream()
	a.tr = &traceStreamState{f: f}
	data := func(id string, n int) {
		if !traceEnqueue(a, &pb.TCPData{SessionId: id, Data: make([]byte, n)}, nil) {
			t.Fatal("not enqueued")
		}
	}
	// One writer per session (pumpToAgent, an inProcConn under wmu):
	// each chunk's Acquire, Write and SendData in turn.
	traceWrite(a, "1", 3)
	data("1", 3)
	traceFinish(a, "1", func() {})
	traceWrite(a, "1", 4) // its Acquire won the race with Finish
	data("1", 4)
	traceWrite(a, "2", 5) // logged before Finish: its SendData stays
	traceFinish(a, "2", func() {})
	data("2", 5)
	b, err := os.ReadFile(f.Name())
	if err != nil {
		t.Fatal(err)
	}
	want := `{"ev":"Write","s":"1","n":3,"t":""}
{"ev":"SendData","s":"1","n":3,"t":""}
{"ev":"Finish","s":"1","n":0,"t":""}
{"ev":"Write","s":"2","n":5,"t":""}
{"ev":"Finish","s":"2","n":0,"t":""}
{"ev":"SendData","s":"2","n":5,"t":""}
`
	if got := string(b); got != want {
		t.Fatalf("trace:\n%s\nwant:\n%s", got, want)
	}
}
