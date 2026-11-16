// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/nf1"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// anonSettings are the nf1.anon.* a test sets; the zero fields keep the
// defaults.
type anonSettings struct {
	legBytes, bucket int
	legTime, life    time.Duration
	sourceMax        int
}

// enableNF1Anon turns nf1 with anonymous addresses on for the relays the
// test starts (before relaycoretest.New), the bucket never refilling (the
// model's address budget), and back to the defaults after.
func enableNF1Anon(t *testing.T, s anonSettings) {
	enableNF1(t, 64)
	set := map[string]any{"nf1.anon.enabled": true, "nf1.anon.bucket_rate": 0}
	if s.legBytes > 0 {
		set["nf1.anon.leg_bytes"] = s.legBytes
	}
	if s.bucket > 0 {
		set["nf1.anon.bucket_bytes"] = s.bucket
	}
	if s.legTime > 0 {
		set["nf1.anon.leg_time"] = s.legTime
	}
	if s.life > 0 {
		set["nf1.anon.lifetime"] = s.life
	}
	if s.sourceMax > 0 {
		set["nf1.anon.source_max"] = s.sourceMax
	}
	defaults := map[string]any{"nf1.anon.enabled": false, "nf1.anon.bucket_rate": 1024, "nf1.anon.leg_bytes": 128 << 10,
		"nf1.anon.bucket_bytes": 1 << 20, "nf1.anon.leg_time": 2 * time.Minute, "nf1.anon.lifetime": time.Hour, "nf1.anon.source_max": 8}
	for k, v := range set {
		viper.Set(k, v)
	}
	t.Cleanup(func() {
		for k := range set {
			viper.Set(k, defaults[k])
		}
	})
}

// startAnonAgent serves "clip" (an echo) under a fresh address with no
// credential: only its registration proof.
func startAnonAgent(t *testing.T, grpcAddr string) nf1Agent {
	t.Helper()
	l, key, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	a, err := connectNF1Agent(t, grpcAddr, "", "clip", l, l.RegistrationProof())
	if err != nil {
		t.Fatal(err)
	}
	return nf1Agent{l: l, key: key, agent: a}
}

func proxyOpts(r *relaycoretest.Relay) nf1.Options {
	return nf1.Options{DialRelay: func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", r.Proxy)
	}}
}

// pump writes n bytes to c while it reads its echo: the bytes echoed and
// the error that ended the read (nil: all of them came back).
func pump(c net.Conn, n int) (int, error) {
	go func() { _, _ = c.Write(make([]byte, n)) }()
	_ = c.SetReadDeadline(time.Now().Add(30 * time.Second))
	got, err := io.CopyN(io.Discard, c, int64(n))
	return int(got), err
}

// isAccountRequired reports whether err is the relay's cut: its abort
// code mid-session, its message at the open (nf1.Dial's refusal).
func isAccountRequired(err error) bool {
	var a *link.Abort
	return err != nil && (errors.As(err, &a) && a.Code == dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_ACCOUNT_REQUIRED) ||
		strings.Contains(err.Error(), nf1anon.Msg))
}

// anonMetric is the value of the dispatcher's nf1_anon_<name> series whose
// one label (if any) is value: the relay's counts of anonymous
// registrations and legs (platform/metrics metrics_nf1anon.go).
func anonMetric(t *testing.T, name, value string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "rpc_server_dispatcher_nf1_anon_"+name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if l := m.GetLabel(); value == "" || len(l) == 1 && l[0].GetValue() == value {
				return m.GetCounter().GetValue() + m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// TestNF1AnonLegBudget (nfltr docs/decisions/nf1-anon-2026-11-16.md
// section 4; spec/NF1Anon.tla LegBounded, TypedFailure): an agent with no
// credential serves its address; a session's relayed leg carries its
// handshake and a small exchange, and a peer writing 200 KiB relayed is cut
// at the leg's budget with ERROR_REASON_ACCOUNT_REQUIRED, having carried no
// more than the budget.
func TestNF1AnonLegBudget(t *testing.T) {
	const leg = 32 << 10
	enableNF1Anon(t, anonSettings{legBytes: leg})
	r := relaycoretest.New(t)
	ag := startAnonAgent(t, r.GRPC)
	c := dialUntil(t, ag.l.Address(), proxyOpts(r))
	defer c.Close() //nolint:errcheck
	echoes(t, c)
	cuts, relayed := anonMetric(t, "cut_total", "bytes"), anonMetric(t, "relayed_bytes_total", "")
	got, err := pump(c, 200<<10)
	if !isAccountRequired(err) {
		t.Fatalf("200 KiB relayed: %v after %d bytes back, want ERROR_REASON_ACCOUNT_REQUIRED", err, got)
	}
	if got > leg/2 {
		t.Fatalf("%d bytes echoed: over the leg's budget (%d both ways)", got, leg)
	}
	if d := anonMetric(t, "cut_total", "bytes") - cuts; d != 1 {
		t.Errorf("nf1_anon_cut_total{reason=bytes} moved by %v, want 1", d)
	}
	if d := anonMetric(t, "relayed_bytes_total", "") - relayed; d <= 0 || d > leg {
		t.Errorf("nf1_anon_relayed_bytes_total moved by %v, want within the leg's %d", d, leg)
	}
}

// TestNF1AnonLegTime: a relayed leg lives at most nf1.anon.leg_time, then
// is cut with ERROR_REASON_ACCOUNT_REQUIRED (spec TimeCut, LeavesRelay).
func TestNF1AnonLegTime(t *testing.T) {
	enableNF1Anon(t, anonSettings{legTime: time.Second})
	r := relaycoretest.New(t)
	ag := startAnonAgent(t, r.GRPC)
	cuts := anonMetric(t, "cut_total", "time")
	c := dialUntil(t, ag.l.Address(), proxyOpts(r))
	defer c.Close() //nolint:errcheck
	_ = c.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.Read(make([]byte, 1)); !isAccountRequired(err) {
		t.Fatalf("a leg past its time: %v, want ERROR_REASON_ACCOUNT_REQUIRED", err)
	}
	if d := anonMetric(t, "cut_total", "time") - cuts; d != 1 {
		t.Errorf("nf1_anon_cut_total{reason=time} moved by %v, want 1", d)
	}
}

// TestNF1AnonSourceBucket: the source's bucket spans sessions and
// registrations (spec AddrBounded): sessions spend it until one is cut,
// and neither a new session nor a new registration (another key, the same
// source) gets it back.
func TestNF1AnonSourceBucket(t *testing.T) {
	enableNF1Anon(t, anonSettings{bucket: 48 << 10})
	r := relaycoretest.New(t)
	ag := startAnonAgent(t, r.GRPC)
	opts := proxyOpts(r)
	cut := func(addr nf1Agent) bool { // a session, cut at its dial or its echo
		t.Helper()
		for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(500 * time.Millisecond) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			c, err := nf1.Dial(ctx, addr.l.Address(), opts)
			cancel()
			if err == nil {
				_, err = pump(c, 8<<10)
				_ = c.Close()
			}
			switch {
			case err == nil:
				return false
			case isAccountRequired(err):
				return true
			case !errors.Is(err, nf1.ErrNotServed) || time.Now().After(deadline):
				t.Fatalf("a session: %v", err)
			}
		}
	}
	n := 0
	for ; n < 20 && !cut(ag); n++ {
	}
	if n == 0 || n == 20 {
		t.Fatalf("%d sessions before the cut: want some, then the bucket's", n)
	}
	if !cut(ag) {
		t.Fatal("a session after the bucket's cut was carried")
	}
	ag.agent.Close()
	again := startAnonAgent(t, r.GRPC)
	if !cut(again) {
		t.Fatal("a new registration from the same source got a full bucket")
	}
}

// TestNF1AnonAccountNotCut: an account's address is never budgeted
// (spec NoCutOnAccount), however small the anonymous limits.
func TestNF1AnonAccountNotCut(t *testing.T) {
	enableNF1Anon(t, anonSettings{legBytes: 4 << 10, bucket: 4 << 10})
	r := relaycoretest.New(t)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r.Key)
	ag := startNF1Agent(t, r.GRPC, keyFile)
	c := dialUntil(t, ag.l.Address(), proxyOpts(r))
	defer c.Close() //nolint:errcheck
	if got, err := pump(c, 1<<20); err != nil {
		t.Fatalf("1 MiB through an account's address: %v after %d bytes", err, got)
	}
}

// TestNF1AnonNotAfter: the registration ends at its NotAfter (spec
// Expire): the relay ends the agent's connection, its relayed legs with
// it, and the agent registers again.
func TestNF1AnonNotAfter(t *testing.T) {
	enableNF1Anon(t, anonSettings{life: 3 * time.Second, legTime: time.Minute})
	r := relaycoretest.New(t)
	expired := anonMetric(t, "registration_events_total", "expired")
	ag := startAnonAgent(t, r.GRPC)
	opts := proxyOpts(r)
	c := dialUntil(t, ag.l.Address(), opts)
	_ = c.SetReadDeadline(time.Now().Add(20 * time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil || isAccountRequired(err) {
		t.Fatalf("a relayed leg at its registration's end: %v, want it ended (expired)", err)
	}
	_ = c.Close()
	c = dialUntil(t, ag.l.Address(), opts)
	echoes(t, c)
	_ = c.Close()
	if d := anonMetric(t, "registration_events_total", "expired") - expired; d < 1 {
		t.Errorf("nf1_anon_registration_events_total{event=expired} moved by %v, want at least 1", d)
	}
}

// TestNF1AnonRegistration: with no credential only proven capability
// addresses register; a plain service or no proof is refused, as is a
// registration over the source's cap (ERROR_REASON_LIMIT_EXCEEDED).
func TestNF1AnonRegistration(t *testing.T) {
	enableNF1Anon(t, anonSettings{sourceMax: 1})
	r := relaycoretest.New(t)
	events := func() [3]float64 {
		return [3]float64{anonMetric(t, "registration_events_total", "registered"), anonMetric(t, "registration_events_total", "refused_proof"),
			anonMetric(t, "registration_events_total", "refused_limit")}
	}
	before, open := events(), anonMetric(t, "registrations", "")
	l, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectNF1Agent(t, r.GRPC, "", "clip", l, nil); status.Code(err) != codes.InvalidArgument {
		t.Errorf("no registration proof: %v, want InvalidArgument", err)
	}
	startAnonAgent(t, r.GRPC)
	l2, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectNF1Agent(t, r.GRPC, "", "clip", l2, l2.RegistrationProof()); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("over the source's cap: %v, want ResourceExhausted", err)
	}
	if after := events(); after[0]-before[0] != 1 || after[1]-before[1] != 1 || after[2]-before[2] != 1 {
		t.Errorf("nf1_anon_registration_events_total registered, refused_proof, refused_limit moved by %v - %v, want 1 each", after, before)
	}
	if d := anonMetric(t, "registrations", "") - open; d != 1 {
		t.Errorf("nf1_anon_registrations moved by %v with one registration open, want 1", d)
	}
}
