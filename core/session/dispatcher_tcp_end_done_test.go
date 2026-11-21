// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// syncClient is a client's session as Join sees it: its CloseWriteSync
// returns what the test sends on synced (the client's far end took the
// end, or not). The test writes the client's bytes into in (closing it is
// the client's half close) and reads what reached the client from out.
type syncClient struct {
	net.Conn // addresses and Close only
	in       *io.PipeReader
	out      *io.PipeWriter
	syncing  chan struct{}
	synced   chan error
}

func (c *syncClient) Read(b []byte) (int, error)  { return c.in.Read(b) }
func (c *syncClient) Write(b []byte) (int, error) { return c.out.Write(b) }
func (c *syncClient) CloseWrite() error           { return c.out.Close() }
func (c *syncClient) CloseWriteSync() error {
	_ = c.out.Close()
	close(c.syncing)
	return <-c.synced
}

// endDoneStream is a stream with the end acknowledgement and the delivery
// report.
func endDoneStream() *tcpAgentStream {
	a := NewTCPAgentStream()
	a.endAck, a.endDone = true, true
	return a
}

// joinedInProc joins a syncClient to an in-process session "s" on a, as
// a client's session reaches an agent on its gRPC streams (onwardLocal,
// then Join). in is the client's writer, out its reader.
func joinedInProc(t *testing.T, a *tcpAgentStream) (cl *syncClient, in *io.PipeWriter, out *io.PipeReader) {
	t.Helper()
	sess := newTCPSession()
	sess.endAck, sess.endDone = a.endAck, a.endDone
	a.addSession("s", sess)
	c := &inProcConn{agentStream: a, sessionID: "s", sess: sess, cleanupFn: func() { a.removeSession("s"); sess.markClosed() }}
	ir, iw := io.Pipe()
	or, ow := io.Pipe()
	p, q := net.Pipe()
	t.Cleanup(func() { _ = p.Close(); _ = q.Close() })
	cl = &syncClient{Conn: p, in: ir, out: ow, syncing: make(chan struct{}), synced: make(chan error, 1)}
	joined := make(chan struct{})
	go func() { defer close(joined); _ = peerlink.Join(cl, c) }()
	t.Cleanup(func() {
		_ = iw.CloseWithError(io.ErrClosedPipe)
		_ = or.Close()
		select {
		case cl.synced <- link.ErrReset:
		default:
		}
		a.CloseAll()
		close(a.Done)
		<-joined
	})
	return cl, iw, or
}

// noDone fails if the relay queues a delivery report within d.
func noDone(t *testing.T, a *tcpAgentStream, d time.Duration, why string) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case f := <-a.SendCh:
			if f.GetSessionId() == dataplane.EndDoneSID {
				t.Fatalf("the relay reported the agent's end delivered %s", why)
			}
		case <-deadline:
			return
		}
	}
}

func isDone(f *pb.TCPData) bool {
	return f.GetSessionId() == dataplane.EndDoneSID && string(f.GetData()) == "s"
}

// TestTCPTunnelEndDoneAfterOnward: the relay reports the agent's half
// close delivered (EndDoneFrame) only once its client's far end took it
// (the client session's CloseWriteSync returned nil), never at the
// relay's own read of it; a client that cannot take it gets no report
// (lane ENDGRPC finding 4; spec/AgentLink.tla DoneTruth).
func TestTCPTunnelEndDoneAfterOnward(t *testing.T) {
	for _, delivered := range []bool{true, false} {
		a := endDoneStream()
		cl, _, out := joinedInProc(t, a)
		a.Dispatch(&pb.TCPData{SessionId: "s", Data: []byte("tail")}, 0)
		a.Dispatch(&pb.TCPData{SessionId: "s", Close: true, Half: true}, 0)
		if b, err := io.ReadAll(out); err != nil || string(b) != "tail" {
			t.Fatalf("the client read %q, %v; want tail, then its end", b, err)
		}
		<-cl.syncing
		noDone(t, a, 300*time.Millisecond, "before its client's far end took it")
		if !delivered {
			cl.synced <- link.ErrReset
			noDone(t, a, 300*time.Millisecond, "although its client's far end did not take it")
			continue
		}
		cl.synced <- nil
		nextFrame(t, a, "delivery report", isDone)
	}
}

// inProcHalf is an in-process session "s" on a whose conn wrote "tail";
// its CloseWriteSyncReceived runs on its own goroutine: received closes at
// the receipt, the result arrives on res.
func inProcHalf(t *testing.T, a *tcpAgentStream) (received chan struct{}, res chan error) {
	t.Helper()
	sess := newTCPSession()
	sess.endAck, sess.endDone = a.endAck, a.endDone
	a.addSession("s", sess)
	c := &inProcConn{agentStream: a, sessionID: "s", sess: sess, cleanupFn: func() { a.removeSession("s"); sess.markClosed() }}
	if _, err := c.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	received, res = make(chan struct{}), make(chan error, 1)
	go func() { res <- c.CloseWriteSyncReceived(func() { close(received) }) }()
	nextFrame(t, a, "half close", isHalf)
	t.Cleanup(func() { _ = c.Close() })
	return received, res
}

// TestInProcCloseWriteSyncWaitsForEndDone: an in-process conn's
// CloseWriteSync passes the agent's receipt on (a Join's hold ends there,
// so crossing half closes cannot wait for each other: mutant
// hold-on-done) and returns nil only at the agent's delivery report; the
// stream's end first is link.ErrReset, a target that takes nothing for
// the idle bound link.ErrEndAckIdle.
func TestInProcCloseWriteSyncWaitsForEndDone(t *testing.T) {
	wait := func(t *testing.T, res chan error) error {
		t.Helper()
		select {
		case err := <-res:
			return err
		case <-time.After(10 * time.Second):
			t.Fatal("CloseWriteSync did not return")
			return nil
		}
	}
	t.Run("report", func(t *testing.T) {
		a := endDoneStream()
		received, res := inProcHalf(t, a)
		a.recvAck(dataplane.EndAckFrame("s"))
		<-received
		select {
		case err := <-res:
			t.Fatalf("CloseWriteSync returned %v at the receipt; want it to wait for the report", err)
		case <-time.After(300 * time.Millisecond):
		}
		a.Dispatch(&pb.TCPData{SessionId: "s", Close: true, Half: true}, 0) // the agent's own end: the session ends cleanly before the report
		a.recvDone(dataplane.EndDoneFrame("s"))
		if err := wait(t, res); err != nil {
			t.Fatalf("CloseWriteSync after the report: %v; want nil", err)
		}
	})
	t.Run("stream ends", func(t *testing.T) {
		a := endDoneStream()
		received, res := inProcHalf(t, a)
		a.recvAck(dataplane.EndAckFrame("s"))
		<-received
		a.CloseAll()
		close(a.Done)
		if err := wait(t, res); !errors.Is(err, link.ErrReset) {
			t.Fatalf("CloseWriteSync after the stream's end: %v; want link.ErrReset", err)
		}
	})
	t.Run("idle", func(t *testing.T) {
		old := endDoneIdle
		endDoneIdle = 200 * time.Millisecond
		t.Cleanup(func() { endDoneIdle = old })
		a := endDoneStream()
		_, res := inProcHalf(t, a)
		a.recvAck(dataplane.EndAckFrame("s"))
		if err := wait(t, res); !errors.Is(err, link.ErrEndAckIdle) {
			t.Fatalf("CloseWriteSync with a mute target: %v; want link.ErrEndAckIdle", err)
		}
	})
	t.Run("old agent", func(t *testing.T) {
		a := NewTCPAgentStream()
		a.endAck = true // no delivery report: nil at the receipt, as a link peer without FeatureEndAck
		received, res := inProcHalf(t, a)
		a.recvAck(dataplane.EndAckFrame("s"))
		<-received
		if err := wait(t, res); err != nil {
			t.Fatalf("CloseWriteSync on a stream without the report: %v; want nil at the receipt", err)
		}
	})
}
