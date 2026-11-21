// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "github.com/nfltr/relay/proto/pb"

// A session's end toward the agent is sent once (spec/AgentLink.tla
// OneEnd, spec/README.md finding 51, lane DOUBLEABORT): handleLocalConn
// and pumpToAgent can each end a visitor's session, and the pump's read
// of a conn handleLocalConn reset fails too.

// abort sends the session's abort (TCPData.abort: never a FIN at the
// agent's target, finding 15) unless its end was sent or taken already,
// and ends it here. Under wmu, so no half close follows it (closeWrite).
func (a *tcpAgentStream) abort(id string, sess *tcpSession) {
	sess.wmu.Lock()
	if !sess.abortSent.Swap(true) {
		a.send(&pb.TCPData{SessionId: id, Close: true, Abort: true})
	}
	sess.wmu.Unlock()
	sess.markClosed()
}

// forgetOnceAcked forgets an inProcConn's session its app closed after
// both half closes once the agent acked its own (or the stream ended):
// with the end acknowledgement both halves end a session only then
// (spec/AgentLink.tla Finish: Held, mutant close-before-ack), as a
// visitor's does (handleLocalConn waits for it).
func (c *inProcConn) forgetOnceAcked() {
	select {
	case <-c.sess.ackCh:
	case <-c.agentStream.Done:
	}
	c.cleanupFn()
}
