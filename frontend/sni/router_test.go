// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/core/sharecode"
	"github.com/nfltr/relay-core/internal/sharestore"
)

// grantsOf is a share store as grants, as the relay hands them its
// frontends (relay.shareGrants); nil for none.
func grantsOf(store sharestore.Store) frontend.GrantVerifier {
	if store == nil {
		return nil
	}
	return storeGrants{store}
}

type storeGrants struct{ store sharestore.Store }

func (g storeGrants) IsCode(s string) bool { return sharecode.Valid(s) }

func (g storeGrants) Grant(code string) (frontend.Grant, error) {
	subject, policy, err := g.store.Lookup(code)
	if errors.Is(err, sharestore.ErrUnknownCode) {
		return frontend.Grant{}, frontend.ErrNoGrant
	}
	if err != nil {
		return frontend.Grant{}, err
	}
	agentID, service, _ := strings.Cut(subject, "/")
	return frontend.Grant{AgentID: agentID, Service: service, Policy: frontend.SharePolicy(policy)}, nil
}

// failingGrants answers every lookup with a store error.
type failingGrants struct{}

func (failingGrants) IsCode(string) bool { return true }
func (failingGrants) Grant(string) (frontend.Grant, error) {
	return frontend.Grant{}, errors.New("dial tcp: connection refused")
}

// TestResolverStoreErrorIsDistinct: a store failure is told apart from an
// unknown code (logged at warn, counted as share_store).
func TestResolverStoreErrorIsDistinct(t *testing.T) {
	store := sharestore.NewInMemory(time.Minute)
	defer store.Close()
	if _, err := newResolver("e2ee.test.local", grantsOf(store)).resolve("aaaa.e2ee.test.local"); !errors.Is(err, errNotAShare) || errors.Is(err, errShareStore) {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := newResolver("e2ee.test.local", failingGrants{}).resolve("aaaa.e2ee.test.local"); !errors.Is(err, errShareStore) {
		t.Fatalf("store down: %v", err)
	}
}

// TestDomainSNIResolver: only a share code under the base domain resolves;
// a host name naming an agent directly does not (agent_reach.go).
func TestDomainSNIResolver(t *testing.T) {
	store := sharestore.NewInMemory(time.Minute)
	defer store.Close()
	code := store.Issue("myapp", 5*time.Minute, sharestore.Policy{})
	tests := []struct {
		name       string
		baseDomain string
		sni        string
		wantErr    bool
	}{
		{name: "strip base domain", baseDomain: "e2ee.example.com", sni: code + ".e2ee.example.com"},
		{name: "leading dot on base domain", baseDomain: ".e2ee.example.com", sni: code + ".e2ee.example.com"},
		{name: "case insensitive", baseDomain: "E2EE.Example.COM", sni: strings.ToUpper(code) + ".e2ee.example.com"},
		{name: "literal agent ID", baseDomain: "e2ee.example.com", sni: "myapp.e2ee.example.com", wantErr: true},
		{name: "no match", baseDomain: "e2ee.example.com", sni: code + ".other.com", wantErr: true},
		{name: "empty prefix", baseDomain: "e2ee.example.com", sni: "e2ee.example.com", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := newResolver(tt.baseDomain, grantsOf(store))
			target, err := resolver.resolve(tt.sni)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve() error = %v, wantErr = %v", err, tt.wantErr)
			}
			if err == nil && (target.AgentID != "myapp" || target.Share != code) {
				t.Errorf("Resolve() = %+v, want agent myapp, share %q", target, code)
			}
		})
	}
	if _, err := newResolver("e2ee.example.com", nil).resolve(code + ".e2ee.example.com"); err == nil {
		t.Fatal("a resolver without a share store resolved a host name")
	}
}

func TestDomainSNIResolver_ShareCode(t *testing.T) {
	store := sharestore.NewInMemory(time.Minute)
	defer store.Close()

	// Issue a share code for an agent.
	code := store.Issue("my-backend-agent", 5*time.Minute, sharestore.Policy{})

	resolver := newResolver("e2ee.example.com", grantsOf(store))

	// Share code should resolve to the agent ID.
	target, err := resolver.resolve(code + ".e2ee.example.com")
	agentID := target.AgentID
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if agentID != "my-backend-agent" {
		t.Errorf("Resolve() = %q, want %q", agentID, "my-backend-agent")
	}

	// A literal agent ID does not resolve.
	if _, err := resolver.resolve("literal-agent.e2ee.example.com"); err == nil {
		t.Fatal("a literal agent ID resolved")
	}

	// A revoked code no longer resolves.
	store.Revoke(code)
	if _, err := resolver.resolve(code + ".e2ee.example.com"); err == nil {
		t.Fatal("a revoked share code resolved")
	}
}

func TestSNIRouter_FullFlow(t *testing.T) {
	logger := slog.Default()

	// Create a TCP dispatcher.
	tcpDisp := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())

	// A share for the agent under the base domain.
	store := sharestore.NewInMemory(time.Minute)
	defer store.Close()
	code := store.Issue("myapp", 5*time.Minute, sharestore.Policy{})

	// Create the SNI router.
	router := NewRouter("e2ee.test.local", grantsOf(store), tcpDisp, logger)

	// Register a fake agent stream directly (same pattern as terminal_test.go).
	agentID := "myapp"
	as := session.NewTCPAgentStream()
	tcpDisp.Mu.Lock()
	tcpDisp.Streams[agentID] = as
	tcpDisp.Mu.Unlock()

	// Start the SNI router on a random port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = router.Serve(ctx, ln)
	}()

	// Give router a moment to start accepting.
	time.Sleep(50 * time.Millisecond)

	// Connect with a TLS ClientHello containing the right SNI.
	sni := code + ".e2ee.test.local"
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Build and send a TLS ClientHello with the SNI.
	clientHello := buildClientHello(t, sni)
	if _, err := conn.Write(clientHello); err != nil {
		t.Fatalf("failed to write ClientHello: %v", err)
	}

	// The SNI router should extract SNI, resolve to "myapp", and send an
	// open frame to the agent via the TCP dispatcher.
	select {
	case frame := <-as.SendCh:
		if !frame.GetOpen() {
			t.Error("expected open frame")
		}
		if frame.GetAgentId() != agentID {
			t.Errorf("agent_id = %q, want %q", frame.GetAgentId(), agentID)
		}
		if !frame.GetE2Ee() {
			t.Error("expected e2ee=true on open frame")
		}
		if frame.GetTargetAddr() != "__e2ee__" {
			t.Errorf("target_addr = %q, want %q", frame.GetTargetAddr(), "__e2ee__")
		}
		// The visitor's address as the relay accepted its connection.
		if frame.GetClientAddr() != "127.0.0.1" {
			t.Errorf("client_addr = %q, want the visitor's 127.0.0.1", frame.GetClientAddr())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for open frame from agent")
	}

	// After the first ClientHello bytes, send some more data.
	testPayload := []byte("more-tls-data")
	if _, err := conn.Write(testPayload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// The dispatcher should forward the data frames.
	select {
	case frame := <-as.SendCh:
		// The data frame should contain the rest of the data stream.
		// The exact bytes depend on buffering, but it should be non-empty.
		if len(frame.GetData()) == 0 {
			t.Error("expected non-empty data frame")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for data frame")
	}

	// Clean up: close conn first to trigger EOF in the reader goroutine,
	// then wait for the resulting close frame before tearing down streams.
	_ = conn.Close()

	// Drain frames until we see the close frame — this confirms the
	// handleLocalConn goroutine has finished calling send().
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
	close(as.Done)
	as.CloseAll()
}

// buildClientHello builds a minimal TLS 1.2 ClientHello with the given SNI.
func buildClientHello(t *testing.T, sni string) []byte {
	t.Helper()

	// Use Go's crypto/tls to produce a real ClientHello by starting a TLS
	// handshake with the SNI set, then capturing the bytes.
	clientR, clientW := net.Pipe()
	serverR, serverW := net.Pipe()

	var clientHello []byte
	var mu sync.Mutex
	done := make(chan struct{})

	// Intercept bytes written by the TLS client.
	go func() {
		defer close(done)
		buf := make([]byte, 16384)
		n, _ := clientR.Read(buf)
		mu.Lock()
		clientHello = make([]byte, n)
		copy(clientHello, buf[:n])
		mu.Unlock()
		_ = clientR.Close()
	}()

	// Start TLS client (will write ClientHello to clientW → piped to clientR).
	go func() {
		tlsConn := tls.Client(clientW, &tls.Config{
			ServerName:         sni,
			InsecureSkipVerify: true, //nolint:gosec // test only
		})
		_ = tlsConn.Handshake() // will fail (no real server) — that's fine
		_ = tlsConn.Close()
		_ = clientW.Close()
	}()

	// We don't need the server side, close it.
	_ = serverR.Close()
	_ = serverW.Close()

	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(clientHello) == 0 {
		t.Fatal("failed to capture ClientHello")
	}
	return clientHello
}
