// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay/client/rpcclient"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Reach (relay-server spec/Reach.tla Allowed): two accounts on one relay,
// alice (machines "alice" and "alice-2") and bob (machine "bob"). Each
// machine's agents are refs <machine id>-<service>.
var (
	aliceHub    = testRef("alice", "hub")
	aliceNode   = testRef("alice", "node")
	aliceLaptop = testRef("alice-2", "laptop")
	bobNode     = testRef("bob", "node")
)

func newTwoAccountDispatcher(t *testing.T, policy Policy) *Dispatcher {
	t.Helper()
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	d.SingleTenant = false
	d.Policy = policy
	for id, owner := range map[string]string{aliceHub: "alice@example.com", aliceNode: "alice@example.com", aliceLaptop: "alice@example.com", bobNode: "bob@example.com"} {
		d.RegisterAgent(id, owner, "")
	}
	return d
}

// machinePrincipal is what a provider states for machine seed of owner.
func machinePrincipal(seed, owner string) identity.Principal {
	m := testMachine(seed)
	return identity.Principal{Provider: "keys", Owner: owner, Machine: m, Subject: string(m)}
}

// groupPolicy is a policy over groups of machines (relay-server's cedar
// policy in miniature): a machine reaches the agents of its own group.
// The group of the caller is read from its snapshot, never p.Group.
type groupPolicy struct {
	mu    sync.Mutex
	group map[identity.MachineID]string
	asked int
}

func (g *groupPolicy) Allowed(p identity.Principal, agent identity.AgentRef, _ Action) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.asked++
	return g.group[p.Machine] != "" && g.group[p.Machine] == g.group[agent.Machine], nil
}

func (g *groupPolicy) set(m identity.MachineID, group string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.group[m] = group
}

// denyAll is a policy that allows nothing.
type denyAll struct{}

func (denyAll) Allowed(identity.Principal, identity.AgentRef, Action) (bool, error) {
	return false, nil
}

func mustReachP(t *testing.T, d *Dispatcher, p identity.Principal, target string) bool {
	t.Helper()
	ok, err := d.Reach(p, target, Dial)
	if err != nil {
		t.Fatalf("Reach(%q): %v", target, err)
	}
	return ok
}

func TestReachFollowsOwnership(t *testing.T) {
	d := newTwoAccountDispatcher(t, AccountIsOneGroup{})
	alice := machinePrincipal("alice", "alice@example.com")
	for _, tc := range []struct {
		name   string
		p      identity.Principal
		target string
		want   bool
	}{
		{"same machine", alice, aliceNode, true},
		{"another machine of the account", alice, aliceLaptop, true},
		{"another account", alice, bobNode, false},
		{"not an agent ref", alice, "alice.node", false},
		{"unauthenticated", identity.Principal{}, bobNode, false},
		{"a principal with no machine", identity.Principal{Provider: "keys", Owner: "alice@example.com", Subject: "x"}, aliceNode, false},
	} {
		if got := mustReachP(t, d, tc.p, tc.target); got != tc.want {
			t.Errorf("%s: Reach(%q) = %v, want %v", tc.name, tc.target, got, tc.want)
		}
	}
	for _, tc := range []struct {
		source, target string
		want           bool
	}{{aliceHub, aliceNode, true}, {aliceHub, aliceLaptop, true}, {aliceHub, bobNode, false}, {"", bobNode, false}} {
		if got := mustReach(t, d.A2AReaches, tc.source, tc.target); got != tc.want {
			t.Errorf("A2AReaches(%q, %q) = %v, want %v", tc.source, tc.target, got, tc.want)
		}
	}
	// Relay plumbing: any authenticated agent reaches the relay's own
	// agent over A2A, nobody reaches it as a tenant's agent.
	if !mustReach(t, d.A2AReaches, bobNode, rpcclient.RelayAgentID) {
		t.Error("an authenticated agent must reach the relay's internal agent over A2A")
	}
	if mustReach(t, d.A2AReaches, "", rpcclient.RelayAgentID) {
		t.Error("no caller reaches nothing, not even the relay's internal agent")
	}
	if ok, _ := d.DialReaches(machinePrincipal("bob", "bob@example.com"))(rpcclient.RelayAgentID); ok {
		t.Error("the relay's internal agent is no tenant's agent")
	}
	d.SingleTenant = true // DEV ONLY: every local caller is the one tenant
	if !mustReachP(t, d, alice, bobNode) || mustReachP(t, d, identity.Principal{}, bobNode) {
		t.Error("trust_loopback: every authenticated caller is the one local tenant, no caller none")
	}
}

// TestReachRefusesOtherGroup (spec NoCrossTenantDial): within one account
// the policy decides by group; another account's agent is refused before
// the policy is asked; no policy refuses everything.
func TestReachRefusesOtherGroup(t *testing.T) {
	g := &groupPolicy{group: map[identity.MachineID]string{testMachine("alice"): "dev", testMachine("alice-2"): "prod", testMachine("bob"): "dev"}}
	d := newTwoAccountDispatcher(t, g)
	alice := machinePrincipal("alice", "alice@example.com")
	alice.Group = "prod" // a stale or forged group on the principal changes nothing
	if !mustReachP(t, d, alice, aliceNode) {
		t.Error("a machine must reach its own group's agents")
	}
	if mustReachP(t, d, alice, aliceLaptop) {
		t.Error("a machine must not reach another group's agent of its account")
	}
	asked := g.asked
	if mustReachP(t, d, alice, bobNode) || g.asked != asked {
		t.Error("another account's agent (bob's dev group) must be refused before the policy is asked")
	}
	d.Policy = nil
	if mustReachP(t, d, alice, aliceNode) {
		t.Error("no policy must refuse (fail closed)")
	}
}

// TestReachRefusesRevokedAfterApply (spec RevokedKeyNoNewSession): Reach
// keeps no answer of its own: once the policy's snapshot drops a machine
// (or moves it), the next session of it is refused on every path.
func TestReachRefusesRevokedAfterApply(t *testing.T) {
	g := &groupPolicy{group: map[identity.MachineID]string{testMachine("alice"): "dev", testMachine("alice-2"): "dev"}}
	d := newTwoAccountDispatcher(t, g)
	laptop := machinePrincipal("alice-2", "alice@example.com")
	if !mustReachP(t, d, laptop, aliceNode) || !mustReach(t, d.A2AReaches, aliceLaptop, aliceNode) {
		t.Fatal("before the revocation the laptop reaches its group")
	}
	g.set(testMachine("alice-2"), "") // revoked: in no group at the new revision
	if mustReachP(t, d, laptop, aliceNode) || mustReach(t, d.A2AReaches, aliceLaptop, aliceNode) {
		t.Error("a revoked machine must open no new session after the revision applied")
	}
	if _, _, err := d.KeyReach(func() TargetRules { return TargetRules{} })(t.Context(), laptop, Target{Agent: aliceNode, Service: "22"}); !refusedNotFound(err) {
		t.Errorf("connect from a revoked machine = %v, want AGENT_NOT_FOUND", err)
	}
}

// TestReachEveryPath: with a policy that denies, every path that opens
// bytes to an agent of the caller's own account is refused (the mutant:
// a path that skips the policy). connect (KeyReach), A2A (dispatch and
// the delivering replica's RelayTunnel), WireGuard's and the direct
// subdomain's DialReaches, and the serving replica's ServeLinkTCP. Browse
// is relay-server's (its own table).
func TestReachEveryPath(t *testing.T) {
	d := newTwoAccountDispatcher(t, denyAll{})
	alice := machinePrincipal("alice", "alice@example.com")

	if _, _, err := d.KeyReach(func() TargetRules { return TargetRules{} })(t.Context(), alice, Target{Agent: aliceNode, Service: "22"}); !refusedNotFound(err) {
		t.Errorf("connect: %v, want AGENT_NOT_FOUND", err)
	}
	if ok, _ := d.DialReaches(alice)(aliceNode); ok {
		t.Error("DialReaches (WireGuard exits, the agent subdomain): allowed")
	}
	if delivered, _ := routeA2A(t, d, aliceHub, aliceNode, true); delivered {
		t.Error("A2A: delivered")
	}

	// The delivering replica: a peer's RelayTunnel to an agent here.
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), PeerLinkCaller{}, true), 5*time.Second) // a delivered call would wait for its answer
	defer cancel()
	targetCh := make(chan *pb.TunnelFrame, 4)
	d.Upstreams.Store(aliceNode, newTestGRPCStream(aliceNode, &channelStreamer{sendCh: targetCh, ctx: ctx}, ctx))
	frame := &pb.TunnelFrame{MessageType: "a2a-request", TransactionId: "txn-peer", Routing: &pb.RoutingInfo{AgentId: aliceHub, TargetAgentId: aliceNode}, Eom: true}
	if err := d.RelayTunnel(&mockRelayTunnelServer{ctx: ctx, recvMsgs: []*pb.TunnelFrame{frame}}); status.Code(err) != codes.NotFound || len(targetCh) != 0 {
		t.Errorf("peer RelayTunnel: %v (%d frames delivered), want NotFound and none", err, len(targetCh))
	}

	// The serving replica: a peer's session a principal opened.
	td := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	td.reach = d.Reach
	for _, tc := range []struct {
		name   string
		reach  func(identity.Principal, string, Action) (bool, error)
		caller identity.Principal
		want   relayv1.ErrorReason
	}{
		{"denied caller", d.Reach, alice, relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND},
		{"no reach wired", nil, alice, relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND},
		{"visitor (no caller)", d.Reach, identity.Principal{}, relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE}, // past the check: no agent here
	} {
		td.reach = tc.reach
		o, in := chainPair(t)
		go td.ServeLinkTCP(in, TCPEndpoint{AgentID: aliceNode, TargetAddr: "22", Caller: tc.caller})
		actx, acancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := o.Answered(actx)
		acancel()
		var ab *link.Abort
		if !errors.As(err, &ab) || answerReason(ab) != tc.want {
			t.Errorf("ServeLinkTCP %s: %v, want %v", tc.name, err, tc.want)
		}
	}
}

// TestAgentToAgentRequiresTargetOwner: the owner's message is delivered;
// another account gets exactly the answer a target that is not connected
// gets.
func TestAgentToAgentRequiresTargetOwner(t *testing.T) {
	d := newTwoAccountDispatcher(t, AccountIsOneGroup{})
	for _, tc := range []struct {
		target string
		want   bool
	}{{aliceNode, true}, {aliceLaptop, true}, {bobNode, false}} {
		delivered, answer := routeA2A(t, d, aliceHub, tc.target, true)
		if delivered != tc.want {
			t.Errorf("alice hub -> %s: delivered = %v, want %v (answer %v)", tc.target, delivered, tc.want, answer)
			continue
		}
		if tc.want {
			continue
		}
		_, notConnected := routeA2A(t, d, aliceHub, testRef("alice", "ghost"), false)
		if answer.GetStatusCode() != int32(codes.NotFound) || answer.GetStatusCode() != notConnected.GetStatusCode() || answer.GetStatusDetail() != notConnected.GetStatusDetail() {
			t.Errorf("refusal (%d %q) differs from the not-connected answer (%d %q)",
				answer.GetStatusCode(), answer.GetStatusDetail(), notConnected.GetStatusCode(), notConnected.GetStatusDetail())
		}
	}
}

// routeA2A sends one A2A request from source to target through d and
// reports whether the target got it, or the answer the source got.
func routeA2A(t *testing.T, d *Dispatcher, source, target string, targetConnected bool) (delivered bool, answer *pb.TunnelFrame) {
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

func refusedNotFound(err error) bool {
	var r *Refusal
	return errors.As(err, &r) && r.Reason == relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND
}

// TestOpenTellsTheAgentItsCaller: a principal's session opens at the agent
// with its machine, group and account (pb.TCPData.Caller, the SDK's
// Session.Caller); the agent decides its own access by them.
func TestOpenTellsTheAgentItsCaller(t *testing.T) {
	td := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	as := NewTCPAgentStream()
	td.Mu.Lock()
	td.Streams[aliceNode] = as
	td.Mu.Unlock()
	c := &Core{TCPDispatcher: td, DialPolicy: func() (int, time.Duration) { return 1, time.Millisecond }}
	c.Reach = func(_ context.Context, _ identity.Principal, tg Target) (string, string, error) {
		return tg.Agent, "22", nil
	}
	alice := machinePrincipal("alice", "alice@example.com")
	alice.Group = "g-dev"
	o, err := c.Open(t.Context(), alice, Target{Agent: aliceNode, Service: "22"})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close() //nolint:errcheck
	select {
	case f := <-as.SendCh:
		if got := f.GetCaller(); got.GetMachineId() != string(alice.Machine) || got.GetGroupId() != "g-dev" || got.GetAccount() != "alice@example.com" {
			t.Fatalf("open frame caller %v, want alice's machine, group and account", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no open frame")
	}
}

// TestServiceConnReachesItsAgent: the relay's own session to a service
// agent (ServiceConn: the mailbox, blob and accounts routes) is the
// agent's machine of its owner's account, which reaches its own agents
// under any policy that keeps a machine's own group; a ref the directory
// records no owner for reaches nothing.
func TestServiceConnReachesItsAgent(t *testing.T) {
	d := newTwoAccountDispatcher(t, &groupPolicy{group: map[identity.MachineID]string{testMachine("alice"): "g"}})
	c := &Core{Dispatcher: d}
	if ok, err := d.Reach(c.servicePrincipal(aliceNode), aliceNode, Dial); err != nil || !ok {
		t.Fatalf("the service route's principal for %s: reach %v, %v", aliceNode, ok, err)
	}
	if p := c.servicePrincipal(aliceNode); p.Owner != "alice@example.com" || p.Machine != testMachine("alice") {
		t.Fatalf("principal %+v, want alice's machine of her account", p)
	}
	if ok, _ := d.Reach(c.servicePrincipal(testRef("carol", "svc")), testRef("carol", "svc"), Dial); ok {
		t.Fatal("an agent with no recorded owner reached")
	}
}

// TestReachOwnerFromMachine (spec AgentIdOwned): an agent's owner is its
// machine's account as MachineOwner states it, so a ref not connected
// yet (a held open) or between sessions is reached by its account and
// by no other; MachineOwner not knowing the machine falls back to the
// directory, and its error fails closed.
func TestReachOwnerFromMachine(t *testing.T) {
	d := newTwoAccountDispatcher(t, AccountIsOneGroup{})
	carolSvc := testRef("carol", "svc") // never connected
	d.MachineOwner = func(m identity.MachineID) (string, error) {
		switch m {
		case testMachine("carol"):
			return "carol@example.com", nil
		case testMachine("broken"):
			return "", errors.New("store down")
		}
		return "", nil
	}
	if !mustReachP(t, d, machinePrincipal("carol-2", "carol@example.com"), carolSvc) {
		t.Error("an offline agent must be reached by its machine's account")
	}
	if mustReachP(t, d, machinePrincipal("alice", "alice@example.com"), carolSvc) {
		t.Error("an offline agent reached by another account")
	}
	if !mustReachP(t, d, machinePrincipal("alice-2", "alice@example.com"), aliceNode) {
		t.Error("a machine MachineOwner does not know must fall back to the directory")
	}
	if ok, err := d.Reach(machinePrincipal("alice", "alice@example.com"), testRef("broken", "x"), Dial); ok || err == nil {
		t.Errorf("a failing MachineOwner: %v, %v; want refused with its error", ok, err)
	}
	if p := (&Core{Dispatcher: d}).servicePrincipal(carolSvc); p.Owner != "carol@example.com" {
		t.Errorf("service principal of an offline agent: owner %q", p.Owner)
	}
}

// providerPolicy records the provider each decision was asked with.
type providerPolicy struct{ asked []string }

func (p *providerPolicy) Allowed(c identity.Principal, _ identity.AgentRef, _ Action) (bool, error) {
	p.asked = append(p.asked, c.Provider)
	return c.Provider != "", nil
}

// TestA2ASourceStatesItsProvider: an A2A source's principal (agentPrincipal,
// read from its owner) carries the provider of its owner form, the
// primary's for an unqualified owner, so a policy that decides by provider
// (relay-server's reach.Policy refuses one none stated) answers A2A as it
// answers the same machine's own dials. Before, the source's provider was
// "": every A2A call within an account refused.
func TestA2ASourceStatesItsProvider(t *testing.T) {
	pol := &providerPolicy{}
	d := newTwoAccountDispatcher(t, pol)
	d.PrimaryProvider = "accounts"
	carolHub, carolNode := testRef("carol", "hub"), testRef("carol", "node")
	d.RegisterAgent(carolHub, "oidc:ab12:carol", "")
	d.RegisterAgent(carolNode, "oidc:ab12:carol", "")
	if !mustReach(t, d.A2AReaches, aliceHub, aliceNode) || !mustReach(t, d.A2AReaches, carolHub, carolNode) {
		t.Fatalf("A2A within an account refused; providers asked %q", pol.asked)
	}
	if want := []string{"accounts", "oidc:ab12"}; len(pol.asked) != 2 || pol.asked[0] != want[0] || pol.asked[1] != want[1] {
		t.Fatalf("providers asked %q, want %q", pol.asked, want)
	}
}
