// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/client/capaddr"
	"github.com/nfltr/relay/client/nf1"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// switchProxy forwards TCP to a target it can change; Switch also cuts
// every connection it carries (a relay restart, as its agents see it).
type switchProxy struct {
	ln     net.Listener
	mu     sync.Mutex
	target string
	conns  map[net.Conn]bool
}

func newSwitchProxy(t *testing.T, target string) *switchProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &switchProxy{ln: ln, target: target, conns: map[net.Conn]bool{}}
	var wg sync.WaitGroup
	t.Cleanup(func() { _ = ln.Close(); p.Switch(""); wg.Wait() })
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			up, err := net.Dial("tcp", p.target)
			if err != nil {
				p.mu.Unlock()
				_ = c.Close()
				continue
			}
			p.conns[c], p.conns[up] = true, true
			p.mu.Unlock()
			wg.Go(func() { _, _ = io.Copy(up, c); _ = up.Close() })
			wg.Go(func() { _, _ = io.Copy(c, up); _ = c.Close() })
		}
	})
	return p
}

func (p *switchProxy) Switch(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
	for c := range p.conns {
		_ = c.Close()
		delete(p.conns, c)
	}
}

func echoListener(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for c, err := ln.Accept(); err == nil; c, err = ln.Accept() {
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	return ln.Addr().String()
}

// nf1Agent is an agent serving "clip" (an echo) under a fresh capability
// address at whatever relay grpc forwards to, its key read from keyFile.
type nf1Agent struct {
	l     *nf1.Listener
	key   ed25519.PrivateKey
	agent agent.Agent
}

func startNF1Agent(t *testing.T, grpcAddr, keyFile string) nf1Agent {
	t.Helper()
	l, key, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	a, err := connectNF1Agent(t, grpcAddr, keyFile, "clip", l, l.RegistrationProof())
	if err != nil {
		t.Fatal(err)
	}
	return nf1Agent{l: l, key: key, agent: a}
}

// connectNF1Agent connects agent agentID serving l's address with the
// registration proof reg (nil: none).
func connectNF1Agent(t *testing.T, grpcAddr, keyFile, agentID string, l *nf1.Listener, reg []byte) (agent.Agent, error) {
	t.Helper()
	table, err := agent.NewServiceTable([]agent.Service{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, Target: echoListener(t), NF1: l}})
	if err != nil {
		t.Fatal(err)
	}
	decl := agentservices.Format([]agentservices.Decl{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, NF1: l.Address().RID}})
	md := []string{agentservices.Header, decl, agentservices.ModesHeader, "1"}
	if reg != nil {
		md = append(md, agentservices.RegHeader, agentservices.FormatReg(l.Address().RID, reg))
	}
	a := agent.New(agent.Config{ServerAddr: grpcAddr, AgentID: agentID, APIKeyFile: keyFile, Logger: slog.New(slog.DiscardHandler)},
		agent.WithMetadata(md...),
		agent.WithConnected(func(agent.Connection) (*agent.Tunnel, error) { return &agent.Tunnel{Services: table}, nil }))
	if err := a.Connect(t.Context()); err != nil {
		a.Close()
		return nil, err
	}
	t.Cleanup(a.Close)
	return a, nil
}

func writeKey(t *testing.T, file, key string) {
	t.Helper()
	if err := os.WriteFile(file, []byte(key), 0o600); err != nil {
		t.Fatal(err)
	}
}

// dialUntil dials addr until it is served (the agent registers its TCP
// stream just after Connect), every 500 ms (under nf1.source_rate).
func dialUntil(t *testing.T, addr capaddr.Address, opts nf1.Options) net.Conn {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(500 * time.Millisecond) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		c, err := nf1.Dial(ctx, addr, opts)
		cancel()
		if err == nil {
			return c
		}
		if !errors.Is(err, nf1.ErrNotServed) || time.Now().After(deadline) {
			t.Fatalf("dial: %v", err)
		}
	}
}

func echoes(t *testing.T, c net.Conn) {
	t.Helper()
	if _, err := c.Write([]byte("nf1")); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 3)
	if _, err := io.ReadFull(c, b); err != nil || string(b) != "nf1" {
		t.Fatalf("echo %q, %v", b, err)
	}
}

// enableNF1 turns nf1 on for host relay.test with addressMax open sessions
// per address for the relays the test starts, and off again once they
// stopped. viper is not safe to change under a running relay: call it
// before relaycoretest.New.
func enableNF1(t *testing.T, addressMax int) {
	viper.Set("nf1.enabled", true)
	viper.Set("nf1.hosts", []string{"relay.test"})
	viper.Set("nf1.address_max", addressMax)
	t.Cleanup(func() { // runs after New's cleanups
		viper.Set("nf1.enabled", false)
		viper.Set("nf1.hosts", nil)
		viper.Set("nf1.address_max", 64)
	})
}

// TestNF1Disabled: a relay that serves no addresses (nf1.enabled off, the
// default) refuses an upgrade with no credential (401) and Dial answers
// ErrNotServed (exit 3), as for an unknown address.
func TestNF1Disabled(t *testing.T) {
	r := relaycoretest.New(t)
	nc, err := net.Dial("tcp", r.Proxy)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close() //nolint:errcheck
	var nu *dataplane.NoUpgradeError
	if _, _, err := dataplane.Upgrade(t.Context(), nc, "relay.test", "", 1<<20); !errors.As(err, &nu) || nu.Status != http.StatusUnauthorized {
		t.Fatalf("an anonymous upgrade: %v, want 401", err)
	}
	l, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = nf1.Dial(t.Context(), l.Address(), nf1.Options{DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", r.Proxy)
	}})
	if !errors.Is(err, nf1.ErrNotServed) {
		t.Fatalf("dial = %v, want ErrNotServed", err)
	}
}

// TestNF1EndToEnd (nfltr docs/decisions/nf1-rendezvous-2026-11-13.md): an
// agent serves a service under a capability address; a client with no
// credential dials the address and its bytes reach the service; another
// server's pin, another address's secret and an unknown rid are refused
// with their typed errors (exit 7, 10, 3); a connection with no
// credential opens nothing else; a
// relay restart drops the registration and the agent's reconnect brings it
// back.
func TestNF1EndToEnd(t *testing.T) {
	enableNF1(t, 64)
	r1 := relaycoretest.New(t)
	grpc := newSwitchProxy(t, r1.GRPC)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r1.Key)
	ag := startNF1Agent(t, grpc.ln.Addr().String(), keyFile)
	l, key := ag.l, ag.key
	relay := r1
	opts := nf1.Options{DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", relay.Proxy)
	}}
	dial := func(addr capaddr.Address) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		return nf1.Dial(ctx, addr, opts)
	}
	echoed := func() {
		t.Helper()
		c := dialUntil(t, l.Address(), opts)
		defer c.Close() //nolint:errcheck
		echoes(t, c)
	}
	echoed()

	other, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	wrongPin := other.Address()
	wrongPin.RID = l.Address().RID // routed to l, which is not its key
	spki, _ := x509.MarshalPKIXPublicKey(key.Public())
	wrongSecret, err := capaddr.New("relay.test", spki) // l's key, another secret
	if err != nil {
		t.Fatal(err)
	}
	wrongSecret.RID = l.Address().RID
	unknown := l.Address()
	unknown.RID[0] ^= 0xff
	for _, tc := range []struct {
		name string
		addr capaddr.Address
		want error
	}{{"pin", wrongPin, capaddr.ErrPin}, {"secret", wrongSecret, capaddr.ErrProof}, {"unknown", unknown, nf1.ErrNotServed}} {
		if c, err := dial(tc.addr); !errors.Is(err, tc.want) {
			if c != nil {
				_ = c.Close()
			}
			t.Errorf("%s: dial = %v, want %v", tc.name, err, tc.want)
		}
	}

	// A connection with no credential opens nf1 sessions only.
	nc, err := net.Dial("tcp", relay.Proxy)
	if err != nil {
		t.Fatal(err)
	}
	m, _, err := dataplane.Upgrade(t.Context(), nc, "relay.test", "", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = m.Serve(nil) }()
	s, err := m.Open(&pb.TCPData{AgentId: relay.Ref("clip"), TargetAddr: "clip", E2Ee: true, Answer: true})
	if err == nil {
		err = s.Answered(t.Context())
	}
	var abort *link.Abort
	if !errors.As(err, &abort) || abort.Code != dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED) {
		t.Errorf("an anonymous keyed open: %v, want UNAUTHENTICATED", err)
	}
	_ = m.Close()

	// The relay restarts (a new one, its store empty): the agent reconnects
	// there and registers the address again.
	r2 := relaycoretest.New(t)
	writeKey(t, keyFile, r2.Key)
	relay = r2
	grpc.Switch(r2.GRPC)
	echoed()
}

// TestNF1CapsAndNotServed: the address's open-session cap refuses (exit 4)
// and frees its count when a session ends; an offline agent's address
// answers exactly as an unknown one (no enumeration, exit 3).
func TestNF1CapsAndNotServed(t *testing.T) {
	enableNF1(t, 1)
	r := relaycoretest.New(t)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r.Key)
	ag := startNF1Agent(t, r.GRPC, keyFile)
	opts := nf1.Options{DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", r.Proxy)
	}}
	first := dialUntil(t, ag.l.Address(), opts)
	echoes(t, first)
	if c, err := nf1.Dial(t.Context(), ag.l.Address(), opts); !errors.Is(err, nf1.ErrRefused) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("a second session over address_max: %v, want ErrRefused", err)
	}
	_ = first.Close()
	var second net.Conn
	for deadline := time.Now().Add(10 * time.Second); second == nil; time.Sleep(500 * time.Millisecond) {
		c, err := nf1.Dial(t.Context(), ag.l.Address(), opts)
		switch {
		case err == nil:
			second = c
		case !errors.Is(err, nf1.ErrRefused) || time.Now().After(deadline):
			t.Fatalf("after the first session ended: %v", err)
		}
	}
	echoes(t, second)
	_ = second.Close()

	unknown := ag.l.Address()
	unknown.RID[0] ^= 0xff
	_, errUnknown := nf1.Dial(t.Context(), unknown, opts)
	ag.agent.Close()
	var errOffline error
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		var c net.Conn
		if c, errOffline = nf1.Dial(t.Context(), ag.l.Address(), opts); errOffline != nil {
			break
		}
		_ = c.Close()
	}
	if !errors.Is(errUnknown, nf1.ErrNotServed) || errOffline == nil || errOffline.Error() != errUnknown.Error() {
		t.Fatalf("offline %v, unknown %v: want one answer", errOffline, errUnknown)
	}
}

// TestNF1TwoServersEndToEnd (nfltr docs/decisions/nf1-risks-2026-11-14.md
// R1-R3): an address holder declaring the address under its own agent is
// refused at its start, with no proof or another key's; two of the owner's
// agents with one key file both serve it, and it stays served while either
// is up.
func TestNF1TwoServersEndToEnd(t *testing.T) {
	enableNF1(t, 64)
	r := relaycoretest.New(t)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r.Key)
	first := startNF1Agent(t, r.GRPC, keyFile)
	opts := nf1.Options{DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", r.Proxy)
	}}
	served := func() {
		t.Helper()
		c := dialUntil(t, first.l.Address(), opts)
		defer c.Close() //nolint:errcheck
		echoes(t, c)
	}
	served()

	other, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	for name, reg := range map[string][]byte{"no proof": nil, "another key's proof": other.RegistrationProof()} {
		if _, err := connectNF1Agent(t, r.GRPC, keyFile, "holder", first.l, reg); status.Code(err) != codes.InvalidArgument {
			t.Errorf("an address holder's declaration, %s: %v, want InvalidArgument", name, err)
		}
	}
	served()

	copied, err := nf1.NewListener(first.l.Address(), first.key) // a copy of the key file
	if err != nil {
		t.Fatal(err)
	}
	second, err := connectNF1Agent(t, r.GRPC, keyFile, "clip2", copied, copied.RegistrationProof())
	if err != nil {
		t.Fatal(err)
	}
	served()
	first.agent.Close()
	served()
	if _, err := connectNF1Agent(t, r.GRPC, keyFile, "clip", first.l, first.l.RegistrationProof()); err != nil {
		t.Fatal(err)
	}
	second.Close()
	served()
}
