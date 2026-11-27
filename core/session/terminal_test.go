// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"

	"github.com/nfltr/relay-core/core/directory"
)

// fakeAgentEcho simulates an agent that echoes all TCP data frames back to
// the session that sent the open frame, then exits when it receives a close.
// It runs in a goroutine and signals done when the open frame is received.
func fakeAgentEcho(t *testing.T, disp *TCPDispatcher, agentID string, agentReady chan struct{}) {
	t.Helper()
	as := NewTCPAgentStream()

	disp.Mu.Lock()
	disp.Streams[agentID] = as
	disp.Mu.Unlock()

	// Signal that the stream is registered.
	close(agentReady)

	// Process frames from the dispatcher (simulating agent logic).
	for frame := range as.SendCh {
		if frame.GetOpen() {
			// Acknowledge the open — nothing to send back for SSH handshake in this stub.
			continue
		}
		if frame.GetClose() {
			return
		}
		if data := frame.GetData(); len(data) > 0 {
			// Echo the data back to the same session.
			as.Mu.Lock()
			sess := as.Sessions[frame.GetSessionId()]
			as.Mu.Unlock()
			if sess == nil {
				continue
			}
			// Deliver directly to recvQ (bypassing the agent → server frame path).
			sess.recvQ.Push(data)
		}
	}
}

func TestDialAgent_Echo(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "test-agent-echo"

	agentReady := make(chan struct{})
	go fakeAgentEcho(t, disp, agentID, agentReady)
	<-agentReady

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := disp.DialAgent(ctx, agentID, "localhost:22")
	if err != nil {
		t.Fatalf("DialAgent: unexpected error: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	want := []byte("hello terminal")
	if _, err := conn.Write(want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	got := make([]byte, len(want))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("echo mismatch: got %q, want %q", got, want)
	}
}

// An agent that sends its whole response and closes at once (vhost E2EE
// without keep-alive) must not lose the tail: DialAgent's reader gets every
// queued byte before EOF.
func TestDialAgent_CloseAfterDataDeliversEverything(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "test-agent-close-after-data"
	as := NewTCPAgentStream()
	disp.Mu.Lock()
	disp.Streams[agentID] = as
	disp.Mu.Unlock()

	chunk := bytes.Repeat([]byte("x"), 32<<10)
	const chunks = 32
	go func() {
		for frame := range as.SendCh {
			if !frame.GetOpen() {
				continue
			}
			as.Mu.Lock()
			sess := as.Sessions[frame.GetSessionId()]
			as.Mu.Unlock()
			for range chunks {
				sess.recvQ.Push(chunk)
			}
			sess.markClosed()
		}
	}()

	conn, err := disp.DialAgent(context.Background(), agentID, "__e2ee__")
	if err != nil {
		t.Fatalf("DialAgent: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	got, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != chunks*len(chunk) {
		t.Fatalf("read %d bytes, want %d", len(got), chunks*len(chunk))
	}
}

// countingWriter records the size of each Write.
type countingWriter struct{ sizes []int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	return len(p), nil
}

// io.Copy from a DialAgent conn (serveConnectStream's download) hands a
// backlog over in Writes of up to tcpWriteBatch: one HTTP/2 DATA frame per
// backlog instead of one per agent frame.
func TestDialAgent_WriteToJoinsBacklog(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "test-agent-writeto"
	as := NewTCPAgentStream()
	disp.Mu.Lock()
	disp.Streams[agentID] = as
	disp.Mu.Unlock()

	chunk := bytes.Repeat([]byte("y"), 32<<10)
	const chunks = 32 // 1 MiB: the session queue's bound, so no push waits
	queued := make(chan struct{})
	go func() {
		for frame := range as.SendCh {
			if !frame.GetOpen() {
				continue
			}
			as.Mu.Lock()
			sess := as.Sessions[frame.GetSessionId()]
			as.Mu.Unlock()
			for range chunks {
				sess.recvQ.Push(chunk)
			}
			sess.markClosed()
			close(queued)
		}
	}()

	conn, err := disp.DialAgent(context.Background(), agentID, "__e2ee__")
	if err != nil {
		t.Fatalf("DialAgent: %v", err)
	}
	defer conn.Close() //nolint:errcheck
	<-queued

	var w countingWriter
	n, err := io.Copy(&w, conn)
	if err != nil || n != chunks*int64(len(chunk)) {
		t.Fatalf("Copy = %d, %v; want %d bytes", n, err, chunks*len(chunk))
	}
	if want := chunks * len(chunk) / tcpWriteBatch; len(w.sizes) != want {
		t.Fatalf("Copy made %d writes %v, want %d of %d bytes", len(w.sizes), w.sizes, want, tcpWriteBatch)
	}
}

func TestDialAgent_AgentNotConnected(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	_, err := disp.DialAgent(context.Background(), "nonexistent-agent", "localhost:22")
	if err == nil {
		t.Fatal("expected error for nonexistent agent, got nil")
	}
	if !strings.Contains(err.Error(), "not connected") {
		t.Errorf("error should mention 'not connected', got: %v", err)
	}
}

func TestDialAgent_CloseReleasesResources(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "test-agent-close"

	agentReady := make(chan struct{})
	go fakeAgentEcho(t, disp, agentID, agentReady)
	<-agentReady

	ctx := context.Background()
	conn, err := disp.DialAgent(ctx, agentID, "localhost:22")
	if err != nil {
		t.Fatalf("DialAgent: %v", err)
	}

	// Close should not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn.Close() //nolint:errcheck
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close() blocked for more than 2 seconds")
	}

	// Read after close must return an error.
	conn.Close() //nolint:errcheck // idempotent
}

func TestInProcConn_Addresses(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "test-agent-addr"

	agentReady := make(chan struct{})
	go fakeAgentEcho(t, disp, agentID, agentReady)
	<-agentReady

	conn, err := disp.DialAgent(context.Background(), agentID, "localhost:22")
	if err != nil {
		t.Fatalf("DialAgent: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	if conn.LocalAddr().Network() != "inproc" {
		t.Errorf("LocalAddr().Network() = %q, want %q", conn.LocalAddr().Network(), "inproc")
	}
	if conn.RemoteAddr().Network() != "inproc" {
		t.Errorf("RemoteAddr().Network() = %q, want %q", conn.RemoteAddr().Network(), "inproc")
	}
	// SetDeadline methods are no-ops but must not error.
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("SetDeadline: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("SetReadDeadline: %v", err)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Errorf("SetWriteDeadline: %v", err)
	}
}

// Verify that the agentStream.sendCh receives the expected open frame when
// DialAgent is called, and that the session ID is non-empty.
func TestDialAgent_OpenFrameSent(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	as := NewTCPAgentStream()
	const agentID = "test-open-frame"
	const target = "10.0.0.1:22"

	disp.Mu.Lock()
	disp.Streams[agentID] = as
	disp.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := disp.DialAgent(ctx, agentID, target)
	if err != nil {
		t.Fatalf("DialAgent: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// Read the open frame that DialAgent should have sent.
	var frame *pb.TCPData
	select {
	case frame = <-as.SendCh:
	case <-time.After(time.Second):
		t.Fatal("no frame received in sendCh within 1 second")
	}

	if !frame.GetOpen() {
		t.Errorf("expected Open=true in first frame, got %v", frame)
	}
	if frame.GetTargetAddr() != target {
		t.Errorf("TargetAddr = %q, want %q", frame.GetTargetAddr(), target)
	}
	if frame.GetAgentId() != agentID {
		t.Errorf("AgentID = %q, want %q", frame.GetAgentId(), agentID)
	}
	if frame.GetSessionId() == "" {
		t.Error("SessionId must not be empty")
	}
}

func TestDialAgentWithRetry_ImmediateSuccess(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "retry-immediate"

	agentReady := make(chan struct{})
	go fakeAgentEcho(t, disp, agentID, agentReady)
	<-agentReady

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := disp.DialAgentWithRetry(ctx, agentID, "localhost:22", 3, 10*time.Millisecond)
	if err != nil {
		t.Fatalf("DialAgentWithRetry: unexpected error: %v", err)
	}
	conn.Close() //nolint:errcheck
}

func TestDialAgentWithRetry_AllAttemptsFail(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := disp.DialAgentWithRetry(ctx, "nonexistent", "localhost:22", 3, 10*time.Millisecond)
	if err == nil {
		t.Fatal("expected error for nonexistent agent, got nil")
	}
	if !strings.Contains(err.Error(), "unreachable after 3 attempts") {
		t.Errorf("error should mention retry exhaustion, got: %v", err)
	}
}

func TestDialAgentWithRetry_SucceedsOnRetry(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "retry-delayed"

	// Register the agent stream after a brief delay so the first attempt
	// fails but a subsequent retry succeeds.
	go func() {
		time.Sleep(30 * time.Millisecond)
		as := NewTCPAgentStream()
		disp.Mu.Lock()
		disp.Streams[agentID] = as
		disp.Mu.Unlock()
		// Drain sendCh so the stream doesn't block.
		go func() {
			for range as.SendCh {
			}
		}()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := disp.DialAgentWithRetry(ctx, agentID, "localhost:22", 5, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("DialAgentWithRetry: expected success on retry, got: %v", err)
	}
	conn.Close() //nolint:errcheck
}

// A connection for an agent that is reconnecting starts as soon as the
// agent's stream registers, not at the next poll of the retry interval.
func TestDialAgentWithRetry_WakesWhenTheAgentRegisters(t *testing.T) {
	t.Parallel()
	disp := NewTCPDispatcher(slog.Default(), nil, nil)
	const agentID = "retry-wakes"
	go func() {
		time.Sleep(50 * time.Millisecond)
		st := &fake.EndableStream{End: make(chan struct{})}
		t.Cleanup(func() { close(st.End) })
		_ = disp.RegisterStream(agentID, st, directory.Session{}, nil)
	}()
	start := time.Now()
	conn, err := disp.DialAgentWithRetry(t.Context(), agentID, "localhost:22", 3, 2*time.Second)
	if err != nil {
		t.Fatalf("DialAgentWithRetry: %v", err)
	}
	_ = conn.Close()
	if took := time.Since(start); took > time.Second {
		t.Fatalf("dial took %v for an agent that registered after 50 ms (retry interval 2 s)", took)
	}
}

func TestDialAgentWithRetry_ContextCancelledDuringRetry(t *testing.T) {
	t.Parallel()

	disp := NewTCPDispatcher(slog.Default(), nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := disp.DialAgentWithRetry(ctx, "never-exists", "localhost:22", 10, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected error when context is cancelled, got nil")
	}
	if !strings.Contains(err.Error(), "context") {
		t.Errorf("error should mention context cancellation, got: %v", err)
	}
}
