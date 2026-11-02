// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"io"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestInProcWriteRacingCloseWriteSendsNothingAfterFIN: a Write waiting for
// credit while another goroutine half-closes the conn sends nothing after
// the half close and reports only the bytes sent before it, with
// io.ErrClosedPipe (spec/README.md finding 1; link's
// TestWriteRacingCloseWriteSendsNothingAfterFIN).
func TestInProcWriteRacingCloseWriteSendsNothingAfterFIN(t *testing.T) {
	a := NewTCPAgentStream()
	a.cr = link.NewCredit(dataplane.MaxChunk, dataplane.MaxChunk, func(string, int) {})
	a.cr.Open("s")
	c := &inProcConn{agentStream: a, sessionID: "s", sess: newTCPSession(), cleanupFn: func() {}}
	type res struct {
		n   int
		err error
	}
	done := make(chan res, 1)
	go func() {
		n, err := c.Write(make([]byte, 2*dataplane.MaxChunk))
		done <- res{n, err}
	}()
	if f := <-a.SendCh; len(f.GetData()) != dataplane.MaxChunk { // the window is sent; the Write waits for the rest's credit
		t.Fatalf("first frame %v, want %d bytes of data", f, dataplane.MaxChunk)
	}
	if err := c.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if f := <-a.SendCh; !f.GetClose() || !f.GetHalf() {
		t.Fatalf("second frame %v, want the half close", f)
	}
	if err := a.cr.Grant("s", dataplane.MaxChunk); err != nil { // the agent read the first chunk
		t.Fatal(err)
	}
	r := <-done
	select {
	case f := <-a.SendCh:
		t.Fatalf("frame after the half close: %d bytes of data", len(f.GetData()))
	default:
	}
	if r.n != dataplane.MaxChunk || !errors.Is(r.err, io.ErrClosedPipe) {
		t.Fatalf("Write returned %d, %v; want %d, io.ErrClosedPipe", r.n, r.err, dataplane.MaxChunk)
	}
}
