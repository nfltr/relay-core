// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"github.com/nfltr/relay/client/nf1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"google.golang.org/grpc/metadata"
)

// TestNF1OpenWaitsForTheDeclaringConnection (spec/NF1Claim.tla Routes,
// Waits; mutants route-by-id, no-wait; lane NF1ROUTE): an nf1 open of an
// address reaches only a stream whose connection declared it. The
// replaced connection of the agent id, declaring another address (its
// agent holds another key), takes no such session: the open waits for the
// declaring connection's stream and lands there once it registers, and
// with none it fails offline at its bound.
func TestNF1OpenWaitsForTheDeclaringConnection(t *testing.T) {
	d := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	d.nf1Hosts = []string{"relay.test"}
	declaring := func() ([agentservices.RIDSize]byte, context.Context) {
		l, _, err := nf1.Mint("relay.test")
		if err != nil {
			t.Fatal(err)
		}
		rid := l.Address().RID
		md := metadata.Pairs(agentservices.Header, agentservices.Format([]agentservices.Decl{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, NF1: rid}}),
			agentservices.RegHeader, agentservices.FormatReg(rid, l.RegistrationProof()))
		return rid, metadata.NewIncomingContext(context.Background(), md)
	}
	register := func(ctx context.Context, seq int64) {
		s := &fake.EndableStream{End: make(chan struct{}), Ctx: ctx}
		t.Cleanup(func() { close(s.End) })
		go func() { _ = d.RegisterStream("clip", s, directory.Session{Seq: seq}, nil) }()
	}
	open := func(ctx context.Context, rid [agentservices.RIDSize]byte) error {
		s, release, err := d.onward(ctx, TCPEndpoint{AgentID: "clip", TargetAddr: "127.0.0.1:1", E2EE: true, NF1: rid})
		if err == nil {
			_ = s.Close()
			if release != nil {
				release()
			}
		}
		return err
	}

	opened := func(as *tcpAgentStream) int {
		as.Mu.Lock()
		defer as.Mu.Unlock()
		return as.opened
	}

	oldRID, oldCtx := declaring()
	newRID, newCtx := declaring()
	register(oldCtx, 1)
	waitStreamsOf(t, d, "clip", 1)
	old := d.agentStream("clip")
	if err := open(context.Background(), oldRID); err != nil {
		t.Fatalf("an open of the address the stream's connection declared: %v", err)
	}

	// No connection that declared the address: offline at the bound.
	bounded, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := open(bounded, newRID); err == nil || !errors.Is(&DialError{Err: err}, ErrOffline) {
		t.Fatalf("an open of an address no stream declared = %v, want offline", err)
	}

	// The window: the open waits for the declaring connection's stream.
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- open(ctx, newRID)
	}()
	before := opened(old)
	register(newCtx, 2)
	if err := <-done; err != nil {
		t.Fatalf("an open in the replacement window: %v, want the new connection's stream", err)
	}
	if cur := d.agentStream("clip"); cur == old || opened(cur) != 1 {
		t.Fatalf("the open's session went to the new connection's stream: %v, %d sessions there", cur != old, opened(cur))
	}
	if opened(old) != before {
		t.Fatal("the replaced connection's stream took a session of an address it did not declare")
	}
}
