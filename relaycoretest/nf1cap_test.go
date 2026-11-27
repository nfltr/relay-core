// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/nf1cap"
	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/client/nf1"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

// startCappedAgent serves "clip" (an echo) under a fresh address with an
// account (keyFile), its label setting the address's cap to capBytes.
func startCappedAgent(t *testing.T, grpcAddr, keyFile string, capBytes int64) *nf1.Listener {
	t.Helper()
	l, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	table, err := agent.NewServiceTable([]agent.Service{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, Target: echoListener(t), NF1: l}})
	if err != nil {
		t.Fatal(err)
	}
	decl := agentservices.Format([]agentservices.Decl{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, NF1: l.Address().RID}})
	a := agent.New(agent.Config{ServerAddr: grpcAddr, AgentID: "clip", TokenFile: keyFile, Logger: slog.New(slog.DiscardHandler)},
		agent.WithLabels(map[string]string{nf1cap.Label("clip"): strconv.FormatInt(capBytes, 10)}),
		agent.WithMetadata(agentservices.Header, decl, agentservices.ModesHeader, "1",
			agentservices.RegHeader, agentservices.FormatReg(l.Address().RID, l.RegistrationProof())),
		agent.WithConnected(func(agent.Connection) (*agent.Tunnel, error) { return &agent.Tunnel{Services: table}, nil }))
	if err := a.Connect(t.Context()); err != nil {
		a.Close()
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	return l
}

// isRelayCap reports whether err is the relay's cap: its abort code
// mid-session, its message at the open (nf1.Dial's refusal).
func isRelayCap(err error) bool {
	var a *link.Abort
	return err != nil && (errors.As(err, &a) && a.Code == dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED) ||
		strings.Contains(err.Error(), nf1cap.Msg))
}

// TestNF1RelayCap (nfltr docs/decisions/nf1-relay-cap-2026-11-25.md;
// spec/NF1Cap.tla CapBounded, TypedFailure, Open): an account's address
// whose agent sets a 64 KiB cap; a client with no account relaying 1 MiB
// is cut with ERROR_REASON_LIMIT_EXCEEDED having carried less than the
// cap, its next open is refused with the cap's message, and a new address
// (--new-address) of the same account starts at zero.
func TestNF1RelayCap(t *testing.T) {
	enableNF1(t, 64)
	viper.Set("nf1.relay_cap", 1<<20) // the default, above the label's: within TLC's integers for the trace
	t.Cleanup(func() { viper.Set("nf1.relay_cap", 4<<30) })
	r := relaycoretest.New(t)
	keyFile := filepath.Join(t.TempDir(), "key")
	writeKey(t, keyFile, r.Key)
	const capBytes = 64 << 10
	l := startCappedAgent(t, r.GRPC, keyFile, capBytes)
	c := dialUntil(t, l.Address(), proxyOpts(r))
	got, err := pump(c, 1<<20)
	_ = c.Close()
	if !isRelayCap(err) {
		t.Fatalf("1 MiB past a 64 KiB cap: %v after %d bytes, want ERROR_REASON_LIMIT_EXCEEDED", err, got)
	}
	if got >= capBytes/2 { // echoed: every byte counted both ways
		t.Fatalf("carried %d bytes each way past a %d byte cap", got, capBytes)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if c, err := nf1.Dial(ctx, l.Address(), proxyOpts(r)); !isRelayCap(err) {
		if c != nil {
			_ = c.Close()
		}
		t.Fatalf("an open at the cap: %v, want the cap's refusal", err)
	}
	// A fresh address under the same agent id replaces the first: until
	// its ConnectTCP stream is up, the open waits for it rather than take
	// the replaced connection's, whose agent holds the old key (lane
	// NF1ROUTE; spec/NF1Claim.tla Routes, Waits).
	fresh := startCappedAgent(t, r.GRPC, keyFile, capBytes)
	c = dialUntil(t, fresh.Address(), proxyOpts(r))
	defer c.Close() //nolint:errcheck
	echoes(t, c)
}
