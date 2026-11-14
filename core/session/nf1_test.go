// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/client/capaddr"
	"github.com/nfltr/relay/client/nf1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// nf1Inventory is a shared inventory over one in-memory store.
func nf1Inventory(t *testing.T) directory.IPInventory {
	t.Helper()
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("POD_IP", "10.0.0.1")
	inv, err := directory.NewIPInventory(slog.New(slog.DiscardHandler), directory.NewMemoryCache(), time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	if err != nil {
		t.Fatal(err)
	}
	return inv
}

// TestNF1DeclarationProof (R1, R5 in nfltr
// docs/decisions/nf1-risks-2026-11-14.md; spec/NF1Claim.tla Accepts): a
// declaration of a capability address is taken only with its server key's
// registration proof for one of the relay's nf1.hosts; without one, with
// another key's, one for another relay, on a relay with no nf1.hosts or
// malformed: InvalidArgument at the agent's start. The proof is never in
// the declaration the relay keeps: not in the record, not in the echo.
func TestNF1DeclarationProof(t *testing.T) {
	owner, key, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	rid := owner.Address().RID
	decl := agentservices.Format([]agentservices.Decl{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, NF1: rid}})
	reg := func(proof []byte) string { return agentservices.FormatReg(rid, proof) }
	other, _, err := nf1.Mint("relay.test")
	if err != nil {
		t.Fatal(err)
	}
	elsewhere := owner.Address()
	elsewhere.Relay = "other.test"
	otherRelay, err := capaddr.RegistrationProof(key, elsewhere)
	if err != nil {
		t.Fatal(err)
	}
	ctx := func(regs ...string) context.Context {
		md := metadata.Pairs(agentservices.Header, decl)
		for _, r := range regs {
			md.Append(agentservices.RegHeader, r)
		}
		return metadata.NewIncomingContext(context.Background(), md)
	}
	hosts := []string{"relay.test"}
	for _, tc := range []struct {
		name  string
		hosts []string
		regs  []string
	}{
		{"no proof", hosts, nil},
		{"another key's proof", hosts, []string{reg(other.RegistrationProof())}},
		{"a proof for another relay", hosts, []string{reg(otherRelay)}},
		{"no nf1.hosts", nil, []string{reg(owner.RegistrationProof())}},
		{"malformed", hosts, []string{"zz:" + base64.RawURLEncoding.EncodeToString(owner.RegistrationProof())}},
	} {
		if _, err := declaredServices(ctx(tc.regs...), tc.hosts); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: %v, want InvalidArgument", tc.name, err)
		} else if strings.Contains(err.Error(), base64.RawURLEncoding.EncodeToString(owner.RegistrationProof())) {
			t.Errorf("%s: the refusal echoes the proof", tc.name)
		}
	}
	for _, h := range [][]string{hosts, {"other.example", "relay.test"}} {
		if d, err := declaredServices(ctx(reg(owner.RegistrationProof())), h); err != nil || len(d) != 1 || d[0].NF1 != rid {
			t.Errorf("the owner's, hosts %v: %v %v", h, d, err)
		}
	}

	// Through a ConnectHTTP stream: refused at its start without a proof;
	// the owner's taken, its proof in neither the echo nor the record.
	d, cancel := newTestDispatcher(t)
	defer cancel()
	inv := nf1Inventory(t)
	d.IPInventory, d.Config.NF1Hosts = inv, hosts
	connect := func(agentID string, regs ...string) (metadata.MD, error) {
		client, cctx := dialDispatcher(t, serveDispatcherAs(t, d, agentID))
		cctx = metadata.AppendToOutgoingContext(cctx, agentservices.Header, decl)
		for _, r := range regs {
			cctx = metadata.AppendToOutgoingContext(cctx, agentservices.RegHeader, r)
		}
		stream, err := client.ConnectHTTP(cctx)
		if err != nil {
			return nil, err
		}
		md, err := stream.Header()
		if err == nil && len(md.Get(agentservices.Header)) == 0 { // refused: the status follows
			_, err = stream.Recv()
		}
		return md, err
	}
	if _, err := connect("holder"); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("an address holder's declaration: %v, want InvalidArgument", err)
	}
	md, err := connect("owner", reg(owner.RegistrationProof()))
	if err != nil {
		t.Fatal(err)
	}
	proof := base64.RawURLEncoding.EncodeToString(owner.RegistrationProof())
	if echo := md.Get(agentservices.Header); len(echo) != 1 || strings.Contains(echo[0], proof) || len(md.Get(agentservices.RegHeader)) != 0 {
		t.Errorf("the echo %v carries the proof", md)
	}
	_, rec, err := inv.(directory.SessionStore).GetSession(context.Background(), "owner")
	if err != nil || rec.Services == "" || strings.Contains(rec.Services, proof) {
		t.Errorf("the record's services %q (%v): want the declaration, no proof", rec.Services, err)
	}
	if got, err := inv.(directory.NF1Index).NF1Agents(context.Background(), rid); err != nil || !slices.Equal(got, []string{"owner"}) {
		t.Errorf("serving %v (%v), want only the owner", got, err)
	}
}

// TestNF1TwoServers (R2, R3; spec/NF1Claim.tla Decide, Dial): two agents
// with one key file both serve the address; an open goes to the newest
// claim; the newest stopped mid-open falls back to the other; while either
// is up the address is served; with neither, not served.
func TestNF1TwoServers(t *testing.T) {
	ctx := context.Background()
	inv := nf1Inventory(t)
	store := inv.(directory.SessionStore)
	rid := [agentservices.RIDSize]byte{0xab, 1}
	decl := agentservices.Format([]agentservices.Decl{{Name: "clip", Kind: agentservices.TCP, E2EE: agentservices.E2EEOn, NF1: rid}})
	recs := map[string]directory.Session{}
	t0 := time.Now()
	claim := func(agent string, since time.Time) {
		t.Helper()
		s, err := store.ClaimSession(ctx, agent, directory.Session{Since: since, Services: decl}, nil, nil, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		recs[agent] = s
	}
	stop := func(agent string) {
		if err := inv.(interface {
			DeleteSession(context.Context, string, directory.Session) error
		}).DeleteSession(ctx, agent, recs[agent]); err != nil {
			t.Error(err)
		}
	}

	var mu sync.Mutex
	var dialled []string
	failing := map[string]bool{} // the agent stops when dialled
	td := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, directory.NewNoOpInventory())
	td.Links = linkOpenerFunc(func(_ context.Context, ep TCPEndpoint) (PeerSession, error) {
		mu.Lock()
		defer mu.Unlock()
		dialled = append(dialled, ep.AgentID)
		if failing[ep.AgentID] {
			stop(ep.AgentID)
			return nil, errors.New("agent gone")
		}
		return &overlapSession{}, nil
	})
	d := &Dispatcher{Agents: &util.SyncMap[string, *HTTPStream]{}, IPInventory: inv, Logger: slog.New(slog.DiscardHandler)}
	c := &Core{Dispatcher: d, TCPDispatcher: td, DialPolicy: func() (int, time.Duration) { return 1, time.Millisecond }}
	c.Reach = func(context.Context, identity.Principal, Target) (string, string, error) { return "", "", ErrRefused } // an nf1 open never asks
	open := func(want string, wantDialled ...string) {
		t.Helper()
		dialled = nil
		out, err := c.Open(ctx, identity.Principal{}, Target{NF1: rid, E2EE: true})
		var r *Refusal
		switch {
		case want == "" && (!errors.As(err, &r) || r.Msg != NF1NotServed):
			t.Errorf("open = %v, want not served", err)
		case want != "" && (err != nil || out.AgentID != want):
			t.Errorf("open reached %q (%v), want %s", out.AgentID, err, want)
		}
		if !slices.Equal(dialled, wantDialled) {
			t.Errorf("dialled %v, want %v", dialled, wantDialled)
		}
	}

	claim("o1", t0)
	claim("o2", t0.Add(time.Second))
	open("o2", "o2") // newest first
	failing["o2"] = true
	open("o1", "o2", "o1") // o2 stopped mid-open: the next
	open("o1", "o1")
	failing["o2"] = false
	claim("o2", t0.Add(2*time.Second))
	stop("o1")
	open("o2", "o2") // either one up: served
	stop("o2")
	open("") // none left: not served, nothing dialled

	// Every live server's dial fails: the last dial's error, for the
	// frontend's log (it answers not served).
	claim("o1", t0.Add(3*time.Second))
	claim("o2", t0.Add(4*time.Second))
	failing["o1"], failing["o2"] = true, true
	dialled = nil
	_, err := c.Open(ctx, identity.Principal{}, Target{NF1: rid, E2EE: true})
	var dial *DialError
	if !errors.As(err, &dial) || dial.AgentID != "o1" || !slices.Equal(dialled, []string{"o2", "o1"}) {
		t.Errorf("every dial failing: %v after %v, want o1's DialError after o2, o1", err, dialled)
	}
}
