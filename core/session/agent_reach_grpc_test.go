// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/internal/platform/auth"
	"github.com/nfltr/relay/client/rpcclient"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// The gRPC paths that send bytes to an agent apply the HTTP side's
// ownership rule (agentCredentialReaches). Two
// accounts on one hosted relay: alice (keys "alice" and "alice-2") and bob
// (key "bob"). Agents connect as "<key>.<name>"; each one's owner is its
// principal's (security.Accounts: the account of its key), recorded when it
// connects.

func newTwoAccountDispatcher(t *testing.T) *Dispatcher {
	t.Helper()
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	d.SingleTenant = false
	owners := map[string]string{"alice": "alice@example.com", "alice-2": "alice@example.com", "bob": "bob@example.com"}
	for id, base := range map[string]string{"alice.hub": "alice", "alice.node": "alice", "alice-2.laptop": "alice-2", "bob.node": "bob"} {
		d.RegisterAgent(id, base, owners[base], "")
	}
	return d
}

// keyCallerCtx is what the API-key interceptor leaves in a caller's context.
// allow:wrapper
func keyCallerCtx(agentID, base string) context.Context {
	return testPrincipal(metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("x-apikey-agentID", agentID, "x-apikey-base-id", base, "x-auth-method", "apikey")))
}

type reachCase struct {
	name   string
	ctx    context.Context
	target string
	want   bool
}

func twoAccountReachCases() []reachCase {
	alice := keyCallerCtx("alice.hub", "alice")
	return []reachCase{
		{"same key", alice, "alice.node", true},
		{"another key of the same account", alice, "alice-2.laptop", true},
		{"another account", alice, "bob.node", false},
		{"key prefix that is not the key", alice, "alicex.node", false},
		{"unauthenticated", context.Background(), "bob.node", false},
	}
}

// callerOf is the agent id and key base the interceptors left in ctx.
func callerOf(ctx context.Context) (agentID, base string) {
	agentID, _ = auth.GetAgentIDFromContext(ctx)
	base = agentID
	if v := firstIncoming(ctx, "x-apikey-base-id"); v != "" {
		base = v
	}
	return agentID, base
}

func TestAgentReachesFollowsOwnership(t *testing.T) {
	d := newTwoAccountDispatcher(t)
	for _, tc := range twoAccountReachCases() {
		caller, base := callerOf(tc.ctx)
		if got := mustReach(t, d.agentReaches, caller, base, tc.target); got != tc.want {
			t.Errorf("%s: agentReaches(%q, %q) = %v, want %v", tc.name, caller, tc.target, got, tc.want)
		}
		if got := mustReach(t, d.A2AReaches, caller, base, tc.target); got != tc.want {
			t.Errorf("%s: a2aReaches(%q, %q) = %v, want %v", tc.name, caller, tc.target, got, tc.want)
		}
	}
	// Relay plumbing: any authenticated agent reaches the relay's own
	// agent over A2A, nobody reaches it as a tenant's agent.
	if !mustReach(t, d.A2AReaches, "bob.node", "bob", rpcclient.RelayAgentID) {
		t.Error("an authenticated agent must reach the relay's internal agent over A2A")
	}
	if mustReach(t, d.A2AReaches, "", "", rpcclient.RelayAgentID) {
		t.Error("no caller reaches nothing, not even the relay's internal agent")
	}
	if mustReach(t, d.agentReaches, "bob.node", "bob", rpcclient.RelayAgentID) {
		t.Error("the relay's internal agent is no tenant's agent")
	}
}

// nfltr relay serve: one account key, no user store, gRPC authz off. Its
// agents still reach each other; a second key is a second namespace (the
// key file provider makes each key its own owner).
func TestAgentReachesWithoutUserStore(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.SingleTenant = false
	d.RegisterAgent("admin.hub", "admin", "keys:admin", "")
	d.RegisterAgent("admin.node", "admin", "keys:admin", "")
	d.RegisterAgent("other.hub", "other", "keys:other", "")
	if !mustReach(t, d.agentReaches, "admin.hub", "admin", "admin.node") {
		t.Error("relay serve: the account's agents must reach each other")
	}
	if mustReach(t, d.agentReaches, "other.hub", "other", "admin.node") {
		t.Error("without a user store another key must not reach the account's agents")
	}
	d.SingleTenant = true // DEV ONLY: every local caller is the one tenant
	if !mustReach(t, d.agentReaches, "worker-1", "worker-1", "worker-2") || mustReach(t, d.agentReaches, "", "", "worker-2") {
		t.Error("trust_loopback: every authenticated caller is the one local tenant, no caller none")
	}
}

// A2A: the owner's message is delivered; another account gets exactly the
// answer a target that is not connected gets.
func TestAgentToAgentRequiresTargetOwner(t *testing.T) {
	route := func(t *testing.T, d *Dispatcher, source, target string, targetConnected bool) (delivered bool, answer *pb.TunnelFrame) {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		targetCh := make(chan *pb.TunnelFrame, 4)
		if targetConnected {
			d.Upstreams.Store(target, newTestGRPCStream(target, &channelStreamer{sendCh: targetCh, ctx: ctx}, ctx))
			defer d.Upstreams.Delete(target)
		}
		sourceCh := make(chan *pb.TunnelFrame, 4)
		sourceStream := newTestGRPCStream(source, &channelStreamer{sendCh: sourceCh, ctx: ctx}, ctx)
		d.Upstreams.Store(source, sourceStream)
		defer d.Upstreams.Delete(source)
		d.dispatchAgentToAgent(ctx, sourceStream, &pb.TunnelFrame{
			Payload: []byte("hi"), MessageType: "a2a-request", TransactionId: "txn-" + source + "-" + target,
			Timestamps: &pb.Timestamps{}, Routing: &pb.RoutingInfo{AgentId: source, TargetAgentId: target}, Eom: true,
		})
		select {
		case <-targetCh:
			return true, nil
		case answer = <-sourceCh:
			return false, answer
		case <-time.After(5 * time.Second):
			t.Fatalf("%s -> %s: neither delivered nor answered", source, target)
			return false, nil
		}
	}

	d := newTwoAccountDispatcher(t)
	for _, tc := range []struct {
		target string
		want   bool
	}{{"alice.node", true}, {"alice-2.laptop", true}, {"bob.node", false}} {
		delivered, answer := route(t, d, "alice.hub", tc.target, true)
		if delivered != tc.want {
			t.Errorf("alice.hub -> %s: delivered = %v, want %v (answer %v)", tc.target, delivered, tc.want, answer)
			continue
		}
		if tc.want {
			continue
		}
		_, notConnected := route(t, d, "alice.hub", "alice.ghost", false)
		if answer.GetStatusCode() != int32(codes.NotFound) || answer.GetStatusCode() != notConnected.GetStatusCode() || answer.GetStatusDetail() != notConnected.GetStatusDetail() {
			t.Errorf("refusal (%d %q) differs from the not-connected answer (%d %q)",
				answer.GetStatusCode(), answer.GetStatusDetail(), notConnected.GetStatusCode(), notConnected.GetStatusDetail())
		}
	}
}
