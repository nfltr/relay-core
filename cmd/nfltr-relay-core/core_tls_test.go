// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/tlsconfig"
	"github.com/nfltr/relay/client/agent"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// writeCert writes a self-signed certificate for names into dir and
// returns its files and DER.
func writeCert(t *testing.T, dir, name string, names ...string) (certFile, keyFile string, der []byte) {
	t.Helper()
	certPEM, keyPEM, err := tlsconfig.SelfSignedPEM(name, names, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	certFile, keyFile = filepath.Join(dir, name+".crt"), filepath.Join(dir, name+".key")
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, c.Certificate[0]
}

// TestCoreProcessTLSAndE2EE: nfltr-relay-core with the operator's
// certificate serves its agent and client ports over TLS only; under
// tcp.e2ee_policy=require it refuses a session the relay could read and
// carries one end to end encrypted, TLS ended at the agent, whose
// certificate the client pins (the relay sees ciphertext only).
func TestCoreProcessTLSAndE2EE(t *testing.T) {
	dir := t.TempDir()
	relayCert, relayKey, relayDER := writeCert(t, dir, "relay", "127.0.0.1", "localhost")
	agentCert, agentKey, agentDER := writeCert(t, dir, "web", "web")
	p := startCoreProcess(t, "TLS_ENABLE=true", "TLS_CERT_FILE="+relayCert, "TLS_KEY_FILE="+relayKey, "TCP_E2EE_POLICY=require")
	roots := x509.NewCertPool()
	rc, err := x509.ParseCertificate(relayDER)
	if err != nil {
		t.Fatal(err)
	}
	roots.AddCert(rc)
	relayTLS := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	serveAgent(t, p.grpc, relayTLS, agent.WithTLSCert(agentCert, agentKey))
	key := testKey[len("alice:"):]

	// Plaintext reaches neither port's service.
	if res, err := http.Get("http://" + p.proxy + "/hello"); err == nil {
		_ = res.Body.Close()
		if res.StatusCode != http.StatusBadRequest {
			t.Fatalf("plaintext request to the TLS proxy port: %d", res.StatusCode)
		}
	}
	if _, err := dialData(t, p.proxy, key); err == nil {
		t.Fatal("a plaintext data connection was taken on the TLS proxy port")
	}

	m, err := dialData(t, p.proxy, key, relayTLS)
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256(agentDER)
	clientTLS := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // the agent's certificate is pinned in VerifyConnection
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 || sha256.Sum256(cs.PeerCertificates[0].Raw) != pin {
				return errors.New("not the agent's pinned certificate")
			}
			return nil
		},
	}
	var answer string
	deadline := time.Now().Add(15 * time.Second)
	for { // the agent registers its TCP stream asynchronously
		answer, err = e2eeGet(m, clientTLS, "/sealed")
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil || answer != "web saw /sealed key=" {
		t.Fatalf("end-to-end encrypted session: %q, %v", answer, err)
	}

	// tcp.e2ee_policy=require: a session the relay could read is refused.
	s, err := m.Open(&pb.TCPData{AgentId: "web", TargetAddr: "22"})
	if err == nil {
		_, _ = s.Write([]byte("?"))
		if n, err := s.Read(make([]byte, 8)); n != 0 || !errors.Is(err, link.ErrOpenRefused) {
			t.Fatalf("a plain session under e2ee_policy=require read %d, %v; want the relay's refusal", n, err)
		}
		_ = s.Close()
	}
}

// e2eeGet opens an end-to-end encrypted session to agent web on m, ends
// TLS with the agent under clientTLS and sends GET path through it.
func e2eeGet(m *dataplane.Mux, clientTLS *tls.Config, path string) (string, error) {
	s, err := m.Open(&pb.TCPData{AgentId: "web", TargetAddr: "22", E2Ee: true})
	if err != nil {
		return "", err
	}
	c := tls.Client(s, clientTLS)
	defer c.Close() //nolint:errcheck
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, "GET "+path+" HTTP/1.1\r\nHost: web\r\nConnection: close\r\n\r\n"); err != nil {
		return "", err
	}
	res, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		return "", err
	}
	defer res.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(res.Body)
	return string(body), err
}

// TestCoreRefusesPlaintextOffLoopback: as nfltr-relay serve, the core
// does not listen off loopback without TLS.
func TestCoreRefusesPlaintextOffLoopback(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "serve", "--from-env")
	cmd.Env = coreEnv(t, "LISTEN_ADDRESS=0.0.0.0")
	var stdout, stderr strings.Builder
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err = cmd.Run()
	if code := cmd.ProcessState.ExitCode(); code != 1 || !strings.Contains(stderr.String(), "refusing to listen on 0.0.0.0 without TLS") {
		t.Fatalf("exit %d (%v); stderr %q; stdout %q", code, err, stderr.String(), stdout.String())
	}
}
