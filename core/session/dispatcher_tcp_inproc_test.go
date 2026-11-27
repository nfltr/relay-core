// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/proto/pb"
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

// TestInProcWriteReturnsOnceClosed: a Write waiting for room in the
// stream's outbox (the agent reads nothing more, its stream stays up)
// returns once its conn is closed or its session ends (spec/README.md
// finding 3, WriteReturns). Before, it waited in enqueue holding the
// session's wmu, so Close waited for it too: relay-server's leakcheck
// found the Write of a link session's drain left behind.
func TestInProcWriteReturnsOnceClosed(t *testing.T) {
	for _, end := range []struct {
		name string
		do   func(a *tcpAgentStream, c *inProcConn)
	}{
		{"close", func(_ *tcpAgentStream, c *inProcConn) { go func() { _ = c.Close() }() }},
		{"stream-ended", func(a *tcpAgentStream, _ *inProcConn) { a.CloseAll() }},
	} {
		t.Run(end.name, func(t *testing.T) {
			a := NewTCPAgentStream() // no credit: the outbox alone holds the Write back
			defer close(a.Done)      // the stream ends after the test: Close's own frame waits for the outbox until then
			sess := newTCPSession()
			a.addSession("s", sess)
			c := &inProcConn{agentStream: a, sessionID: "s", sess: sess, cleanupFn: func() { a.removeSession("s"); sess.markClosed() }}
			for len(a.SendCh) < cap(a.SendCh) {
				a.enqueue(&pb.TCPData{SessionId: "other", Data: []byte{0}})
			}
			done := make(chan error, 1)
			go func() {
				_, err := c.Write([]byte("blocked"))
				done <- err
			}()
			time.Sleep(20 * time.Millisecond) // the Write waits in enqueue (it cannot do otherwise: the outbox is full)
			end.do(a, c)
			select {
			case err := <-done:
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("Write returned %v, want io.ErrClosedPipe", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a Write waiting for the outbox did not return once its session ended")
			}
		})
	}
}
