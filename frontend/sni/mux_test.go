// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/testcert"
	"github.com/nfltr/relay-core/internal/sharestore"
)

func TestSNIMuxListener_E2EERouted(t *testing.T) {
	logger := slog.Default()

	// Set up a TCP dispatcher with a fake agent stream.
	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())
	agentID := "myapp"
	as := session.NewTCPAgentStream()
	tcpDisp.Mu.Lock()
	tcpDisp.Streams[agentID] = as
	tcpDisp.Mu.Unlock()

	store := sharestore.NewInMemory(time.Minute)
	defer store.Close()
	code := store.Issue(agentID, 5*time.Minute, sharestore.Policy{})

	// Create a raw TCP listener.
	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create the SNI mux wrapping the raw listener.
	mux := NewMux(ctx, rawLn, "e2ee.test.local", grantsOf(store), tcpDisp, logger)

	// Start accepting in background — since E2EE conns are handled silently,
	// Accept() should block after routing the E2EE conn.
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		// This Accept call should NOT return for an E2EE conn — it routes
		// internally and loops.  We'll cancel context to unblock.
		_, _ = mux.Accept()
	}()

	// Connect with an E2EE SNI.
	conn, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	clientHello := buildClientHello(t, code+".e2ee.test.local")
	if _, err := conn.Write(clientHello); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The mux should route to the agent — verify open frame.
	select {
	case frame := <-as.SendCh:
		if !frame.GetOpen() {
			t.Error("expected open frame")
		}
		if frame.GetAgentId() != agentID {
			t.Errorf("agent_id = %q, want %q", frame.GetAgentId(), agentID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for open frame")
	}

	_ = conn.Close()

	// Drain until close frame.
	for {
		select {
		case frame := <-as.SendCh:
			if frame.GetClose() {
				goto drained
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for close frame")
		}
	}
drained:

	cancel()
	if err := mux.Close(); err != nil {
		t.Errorf("close mux: %v", err)
	}
	close(as.Done)
	as.CloseAll()
}

func TestSNIMuxListener_NonE2EEReturned(t *testing.T) {
	logger := slog.Default()

	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := NewMux(ctx, rawLn, "e2ee.test.local", nil, tcpDisp, logger)

	// Accept in background.
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		c, err := mux.Accept()
		ch <- acceptResult{c, err}
	}()

	// Connect with a non-E2EE SNI (e.g., grpc.nfltr.xyz).
	conn, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	clientHello := buildClientHello(t, "grpc.nfltr.xyz")
	if _, err := conn.Write(clientHello); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The mux should return this conn from Accept().
	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatalf("accept error: %v", result.err)
		}
		if result.conn == nil {
			t.Fatal("expected non-nil conn")
		}
		if err := result.conn.Close(); err != nil {
			t.Fatalf("close accepted conn: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Accept to return non-E2EE conn")
	}

	cancel()
	if err := mux.Close(); err != nil {
		t.Errorf("close mux: %v", err)
	}
}

func TestSNIMuxListener_NonE2EEDoesNotHitShareStoreForGrpcSubdomain(t *testing.T) {
	logger := slog.Default()

	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())
	store := &countingShareStore{}

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := NewMux(ctx, rawLn, "nfltr.xyz", grantsOf(store), tcpDisp, logger)

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		c, err := mux.Accept()
		ch <- acceptResult{c, err}
	}()

	conn, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	clientHello := buildClientHello(t, "grpc.nfltr.xyz")
	if _, err := conn.Write(clientHello); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatalf("accept error: %v", result.err)
		}
		if result.conn == nil {
			t.Fatal("expected non-nil conn")
		}
		if err := result.conn.Close(); err != nil {
			t.Fatalf("close accepted conn: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Accept to return grpc conn")
	}

	if got := store.validations.Load(); got != 0 {
		t.Fatalf("shareStore.Validate called %d times, want 0", got)
	}

	cancel()
	if err := mux.Close(); err != nil {
		t.Errorf("close mux: %v", err)
	}
}

func TestSNIMuxListener_NotTLSReturned(t *testing.T) {
	logger := slog.Default()

	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := NewMux(ctx, rawLn, "e2ee.test.local", nil, tcpDisp, logger)

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		c, err := mux.Accept()
		ch <- acceptResult{c, err}
	}()

	// Connect with non-TLS data (plain HTTP).
	conn, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// The mux should return this conn from Accept() (SNI extraction fails,
	// conn is passed through).
	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatalf("accept error: %v", result.err)
		}
		if result.conn == nil {
			t.Fatal("expected non-nil conn")
		}
		if err := result.conn.Close(); err != nil {
			t.Fatalf("close accepted conn: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Accept to return non-TLS conn")
	}

	_ = conn.Close()
	cancel()
	if err := mux.Close(); err != nil {
		t.Errorf("close mux: %v", err)
	}
}

// TestSNIMuxListener_WithTLSWrap verifies that a non-E2EE connection
// returned from the SNI mux can be successfully TLS-terminated (simulating
// the gRPC path where tls.NewListener wraps the mux).
func TestSNIMuxListener_WithTLSWrap(t *testing.T) {
	logger := slog.Default()

	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := rawLn.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := NewMux(ctx, rawLn, "e2ee.test.local", nil, tcpDisp, logger)

	// Generate a self-signed TLS cert for the server.
	cert := testcert.Generate(t)
	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	// Wrap the mux output with TLS — this is what the server does.
	tlsLn := tls.NewListener(mux, tlsCfg)

	// Accept TLS-terminated connections in background.
	type acceptResult struct {
		data []byte
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		conn, err := tlsLn.Accept()
		if err != nil {
			ch <- acceptResult{nil, err}
			return
		}
		defer func() { _ = conn.Close() }()

		buf := make([]byte, 64)
		n, _ := conn.Read(buf)
		ch <- acceptResult{buf[:n], nil}
	}()

	// Client connects with a non-E2EE SNI and does a full TLS handshake.
	clientConn, err := tls.Dial("tcp", addr, &tls.Config{
		ServerName:         "grpc.nfltr.xyz",
		InsecureSkipVerify: true, //nolint:gosec // test only
	})
	if err != nil {
		t.Fatalf("tls dial: %v", err)
	}
	defer func() { _ = clientConn.Close() }()

	testMsg := []byte("hello-grpc")
	if _, err := clientConn.Write(testMsg); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Server should receive the decrypted message.
	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatalf("accept/read error: %v", result.err)
		}
		if string(result.data) != "hello-grpc" {
			t.Errorf("got %q, want %q", result.data, "hello-grpc")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for TLS data")
	}

	cancel()
	if err := tlsLn.Close(); err != nil {
		t.Errorf("close TLS listener: %v", err)
	}
}

// TestSNIMuxListener_SlowClientHelloDoesNotBlockAccept: a client that
// connects and sends nothing must not hold up the next client's Accept
// (the ClientHello peek used to run on the accept loop, 5 s per silent
// client, for every connection on the port, gRPC reconnects included).
func TestSNIMuxListener_SlowClientHelloDoesNotBlockAccept(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())
	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := NewMux(t.Context(), rawLn, "e2ee.test.local", nil, tcpDisp, logger)
	defer mux.Close() //nolint:errcheck

	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			c, err := mux.Accept()
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	silent, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()              //nolint:errcheck
	time.Sleep(50 * time.Millisecond) // the silent client is accepted first

	fast, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close() //nolint:errcheck
	if _, err := fast.Write(buildClientHello(t, "grpc.nfltr.xyz")); err != nil {
		t.Fatal(err)
	}

	select {
	case c := <-accepted:
		if c.RemoteAddr().String() != fast.LocalAddr().String() {
			t.Fatalf("accepted %s, want the client that sent its hello (%s)", c.RemoteAddr(), fast.LocalAddr())
		}
		_ = c.Close()
	case <-time.After(time.Second):
		t.Fatal("a silent client blocked Accept for the next one")
	}
}

type countingShareStore struct {
	validations atomic.Int64
}

func (s *countingShareStore) Issue(_ string, _ time.Duration, _ sharestore.Policy) string {
	return ""
}

func (s *countingShareStore) Validate(_ string) (string, sharestore.Policy, bool) {
	s.validations.Add(1)
	return "", sharestore.Policy{}, false
}

func (s *countingShareStore) Lookup(_ string) (string, sharestore.Policy, error) {
	s.validations.Add(1)
	return "", sharestore.Policy{}, sharestore.ErrUnknownCode
}

func (s *countingShareStore) Touch(_ string, _ time.Duration) {}

func (s *countingShareStore) Revoke(_ string) {}

func (s *countingShareStore) RevokeByAgentID(_ string) int {
	return 0
}

func (s *countingShareStore) GetCodesByAgentID(_ string) []string {
	return nil
}

func (s *countingShareStore) Close() {}
