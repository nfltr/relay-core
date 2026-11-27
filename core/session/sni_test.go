// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"sync"
	"testing"
)

func TestPrefixConn(t *testing.T) {
	// Set up a net.Pipe with known data.
	serverR, clientW := net.Pipe()
	defer func() { _ = serverR.Close() }()

	restData := []byte("rest-of-the-data")
	go func() {
		_, _ = clientW.Write(restData)
		_ = clientW.Close()
	}()

	prefix := []byte("prefix-bytes")
	pc := &PrefixConn{
		Conn:   serverR,
		Prefix: prefix,
	}

	// Read should return prefix first, then the rest.
	buf := make([]byte, 256)
	var all []byte
	for {
		n, err := pc.Read(buf)
		if n > 0 {
			all = append(all, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	expected := append([]byte{}, prefix...)
	expected = append(expected, restData...)
	if !bytes.Equal(all, expected) {
		t.Errorf("got %q, want %q", all, expected)
	}
}

func TestExtractSNI(t *testing.T) {
	t.Helper()

	tests := []struct {
		name    string
		sni     string
		wantSNI string
		wantErr error
	}{
		{
			name:    "standard hostname",
			sni:     "myapp.e2ee.example.com",
			wantSNI: "myapp.e2ee.example.com",
		},
		{
			name:    "short hostname",
			sni:     "a.b.c",
			wantSNI: "a.b.c",
		},
		{
			name:    "long hostname",
			sni:     "very-long-hostname.sub.domain.example.org",
			wantSNI: "very-long-hostname.sub.domain.example.org",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Build a minimal TLS ClientHello with the given SNI.
			clientHello := buildClientHello(t, tt.sni)

			// Create a pipe: write ClientHello into one end, read from other.
			serverR, clientW := net.Pipe()
			defer func() { _ = serverR.Close() }()

			go func() {
				_, _ = clientW.Write(clientHello)
				// Don't close — extractSNI should succeed reading just ClientHello.
			}()
			defer func() { _ = clientW.Close() }()

			gotSNI, peeked, err := ExtractSNI(serverR)
			if err != nil {
				t.Fatalf("ExtractSNI() error = %v", err)
			}
			if gotSNI != tt.wantSNI {
				t.Errorf("ExtractSNI() SNI = %q, want %q", gotSNI, tt.wantSNI)
			}
			if len(peeked) == 0 {
				t.Error("ExtractSNI() peeked should not be empty")
			}
		})
	}
}

func TestExtractSNI_NotTLS(t *testing.T) {
	serverR, clientW := net.Pipe()
	defer func() { _ = serverR.Close() }()

	go func() {
		_, _ = clientW.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
		_ = clientW.Close()
	}()

	_, _, err := ExtractSNI(serverR)
	if err != errNotTLS {
		t.Errorf("expected errNotTLS, got %v", err)
	}
}

func TestExtractSNI_Truncated(t *testing.T) {
	serverR, clientW := net.Pipe()
	defer func() { _ = serverR.Close() }()

	go func() {
		// Send just the TLS record header with a huge length but no body.
		_, _ = clientW.Write([]byte{0x16, 0x03, 0x01, 0x00, 0x05})
		_ = clientW.Close()
	}()

	_, _, err := ExtractSNI(serverR)
	if err != errSNITruncated {
		t.Errorf("expected errSNITruncated, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// DomainSNIResolver tests

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
