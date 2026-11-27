// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build linux || darwin

package session

import (
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestInProcCloseAfterBothHalvesHoldsSessionForAck: an in-process conn
// closed after both half closes, its own not yet acked, is forgotten
// only once the agent acked it (spec/AgentLink.tla Finish: Held, finding
// 2's hold), as a visitor's session is (handleLocalConn waits for the ack).
// Forgotten at Close, the trace's Finish came before the ack (lane
// DOUBLEABORT: TestTCPTunnelHalfClose's in-process traces rejected).
func TestInProcCloseAfterBothHalvesHoldsSessionForAck(t *testing.T) {
	a := endDoneStream()
	sess := newTCPSession()
	sess.endAck, sess.endDone = a.endAck, a.endDone
	a.addSession("s", sess)
	removed := make(chan struct{})
	c := &inProcConn{agentStream: a, sessionID: "s", sess: sess, cleanupFn: func() { a.removeSession("s"); sess.markClosed(); close(removed) }}
	a.Dispatch(&pb.TCPData{SessionId: "s", Close: true, Half: true}, 0) // the agent's half close first
	_ = c.CloseWrite()
	_ = c.Close()
	select {
	case <-removed:
		t.Fatal("the session was forgotten at Close, before the agent acked its half close")
	default:
	}
	a.recvAck(dataplane.EndAckFrame("s")) // the receive loop takes the ack (recvFrame)
	select {
	case <-removed:
	case <-time.After(10 * time.Second):
		t.Fatal("the session was not forgotten once the agent acked its half close")
	}
}

// TestTCPTunnelVisitorAbortSendsOneAbort: a visitor's end not known
// delivered aborts the session toward the agent once, though the pump's
// read of the reset visitor fails too: one end per session
// (spec/AgentLink.tla Abort and VisitorAbort need wfin # "whole"; lane
// DOUBLEABORT).
func TestTCPTunnelVisitorAbortSendsOneAbort(t *testing.T) {
	old := endDoneIdle
	endDoneIdle = 300 * time.Millisecond
	t.Cleanup(func() { endDoneIdle = old })
	a := endDoneStream()
	_, sid := visitorSession(t, a, true)
	for off := 0; off < len(stuckTail); off += 16 << 10 {
		a.Dispatch(&pb.TCPData{SessionId: sid, Data: stuckTail[off : off+16<<10]}, 10*time.Second)
	}
	a.Dispatch(&pb.TCPData{SessionId: sid, Close: true, Half: true}, 0)
	isAbort := func(f *pb.TCPData) bool { return f.GetSessionId() == sid && f.GetClose() && !f.GetHalf() }
	nextFrame(t, a, "abort", isAbort)
	// The pump's read fails at once on the reset conn: a second abort
	// would follow within the bound (a pass under load can miss it, a
	// failure is never spurious).
	quiet := time.After(500 * time.Millisecond)
	for {
		select {
		case f := <-a.SendCh:
			if isAbort(f) {
				t.Fatal("a second abort of the session")
			}
		case <-quiet:
			return
		}
	}
}
