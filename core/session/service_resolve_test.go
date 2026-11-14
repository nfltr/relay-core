// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const serviceOwner = "alice@example.com"

// servicePolicy is AccountIsOneGroup that denies some machines and names
// others (NameResolver).
type servicePolicy struct {
	AccountIsOneGroup
	deny  map[identity.MachineID]bool
	names map[string]identity.MachineID
}

func (p *servicePolicy) Allowed(_ identity.Principal, a identity.AgentRef, _ Action) (bool, error) {
	return !p.deny[a.Machine], nil
}

func (p *servicePolicy) ResolveMachine(_ identity.Principal, name string) (identity.MachineID, error) {
	return p.names[name], nil
}

// newServiceDispatcher is a dispatcher whose directory is the shared
// inventory's (both records in one MemoryCache), with policy.
func newServiceDispatcher(t *testing.T, policy Policy) *Dispatcher {
	t.Helper()
	viper.Set("pod_ip.env_var", "TEST_POD_IP") // records name a pod
	t.Cleanup(func() { viper.Set("pod_ip.env_var", "") })
	t.Setenv("TEST_POD_IP", "10.0.0.2")
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	d.SingleTenant, d.Policy = false, policy
	mc := directory.NewMemoryCache()
	d.IPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	d.StreamerIPInventory, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetStreamerCacheKey)
	return d
}

// publish connects the agent on machine seed of account, declaring
// services (its dispatcher record) and labels (its stream record), and
// returns its ref.
func publish(t *testing.T, d *Dispatcher, seed, account, services string, labels map[string]string) string {
	t.Helper()
	ref := testRef(seed, "pgsrv")
	s := directory.Session{Since: time.Now(), Account: account, Services: services}
	if _, err := d.IPInventory.(directory.SessionStore).ClaimSession(t.Context(), ref, s, nil, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := d.StreamerIPInventory.(directory.SessionStore).ClaimSession(t.Context(), ref, directory.Session{Since: time.Now(), Account: account}, labels, nil, time.Minute); err != nil {
		t.Fatal(err)
	}
	return ref
}

const pgAtDB = "pg=tcp+e2ee;group=db"

var pgDown = map[string]string{agentservices.HealthLabel("pg"): agentservices.HealthDown}

// TestResolveServiceSelection (service-discovery section 3 "Instance
// selection"): the relay picks among the instances of the caller's
// account that Reach allows, minus the excluded (by ref or by machine
// name), healthy first, the down ones when no healthy one is left;
// uniform random; none left is AGENT_OFFLINE, whatever hid them.
func TestResolveServiceSelection(t *testing.T) {
	type inst struct {
		seed, account, services string
		down, denied            bool
	}
	up := func(seed string) inst { return inst{seed: seed, account: serviceOwner, services: pgAtDB} }
	down := func(seed string) inst { i := up(seed); i.down = true; return i }
	denied := func(i inst) inst { i.denied = true; return i }
	for _, tc := range []struct {
		name    string
		insts   []inst
		exclude []string // seeds (as refs) or "name:<seed>" (as machine names)
		want    []string // seeds; none: AGENT_OFFLINE
	}{
		{"uniform among the healthy", []inst{up("a"), up("b"), down("c")}, nil, []string{"a", "b"}},
		{"one healthy", []inst{down("a"), up("b"), down("c")}, nil, []string{"b"}},
		{"all down: among the down", []inst{down("a"), down("b")}, nil, []string{"a", "b"}},
		{"reach-denied", []inst{denied(up("a")), up("b")}, nil, []string{"b"}},
		{"excluded by ref", []inst{up("a"), up("b")}, []string{"a"}, []string{"b"}},
		{"excluded by machine name", []inst{up("a"), up("b")}, []string{"name:a"}, []string{"b"}},
		{"the excluded healthy one leaves the down one", []inst{up("a"), down("b")}, []string{"a"}, []string{"b"}},
		{"another account's", []inst{{seed: "a", account: "bob@example.com", services: pgAtDB}}, nil, nil},
		{"not published under the group", []inst{{seed: "a", account: serviceOwner, services: "pg=tcp+e2ee;group=web"}, {seed: "b", account: serviceOwner, services: "pg=tcp+e2ee"}}, nil, nil},
		{"every one excluded", []inst{up("a")}, []string{"a"}, nil},
		{"every one denied", []inst{denied(up("a")), denied(down("b"))}, nil, nil},
		{"none", nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := &servicePolicy{deny: map[identity.MachineID]bool{}, names: map[string]identity.MachineID{}}
			d := newServiceDispatcher(t, policy)
			seedOf := map[string]string{}
			for _, i := range tc.insts {
				var labels map[string]string
				if i.down {
					labels = pgDown
				}
				seedOf[publish(t, d, i.seed, i.account, i.services, labels)] = i.seed
				policy.names[i.seed+"-host"] = testMachine(i.seed)
				if i.denied {
					policy.deny[testMachine(i.seed)] = true
				}
			}
			var exclude []string
			for _, e := range tc.exclude {
				if name, ok := strings.CutPrefix(e, "name:"); ok {
					exclude = append(exclude, name+"-host")
				} else {
					exclude = append(exclude, testRef(e, "pgsrv"))
				}
			}
			caller := machinePrincipal("caller", serviceOwner)
			seen := map[string]bool{}
			for range 100 {
				got, refusal := d.ResolveService(t.Context(), caller, d.DialReaches(caller), AgentAddress{Service: "pg", Group: "db"}, exclude)
				if tc.want == nil {
					if refusal == nil || refusal.Reason != relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE {
						t.Fatalf("picked %q, %v; want AGENT_OFFLINE", got, refusal)
					}
					return
				}
				if refusal != nil || !slices.Contains(tc.want, seedOf[got]) {
					t.Fatalf("picked %q (%s), %v; want one of %v", got, seedOf[got], refusal, tc.want)
				}
				seen[seedOf[got]] = true
			}
			if len(seen) != len(tc.want) {
				t.Fatalf("picked %v over 100 opens, want each of %v", seen, tc.want)
			}
		})
	}
}

// TestResolveServiceTarget: an open of a service by name names its
// service in the target or leaves it empty; another service is refused
// before any instance is picked.
func TestResolveServiceTarget(t *testing.T) {
	d := newServiceDispatcher(t, AccountIsOneGroup{})
	d.TCPDispatcher = NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	ref := publish(t, d, "a", serviceOwner, pgAtDB, nil)
	caller := machinePrincipal("caller", serviceOwner)
	for _, target := range []string{"", "pg"} {
		got, addr, err := d.ResolveTarget(t.Context(), caller, d.DialReaches(caller), Target{Agent: "pg@db", Service: target, E2EE: true}, TargetRules{})
		if err != nil || got != ref || addr != "pg" {
			t.Fatalf("target %q: %q %q %v; want %s's pg", target, got, addr, err, ref)
		}
	}
	var r *Refusal
	if _, _, err := d.ResolveTarget(t.Context(), caller, d.DialReaches(caller), Target{Agent: "pg@db", Service: "web", E2EE: true}, TargetRules{}); !errors.As(err, &r) || r.Reason != relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT {
		t.Fatalf("another service in the target: %v, want INVALID_ARGUMENT", err)
	}
}

// fakeServiceAgent is an agent stream that answers every open with
// reason (0: accepted) and counts them.
type fakeServiceAgent struct {
	as     *tcpAgentStream
	reason relayv1.ErrorReason
	mu     sync.Mutex
	opens  int
}

func (f *fakeServiceAgent) run(ctx context.Context) {
	for {
		select {
		case fr := <-f.as.SendCh:
			if !fr.GetOpen() {
				continue
			}
			f.mu.Lock()
			f.opens++
			f.mu.Unlock()
			f.as.Dispatch(&pb.TCPData{SessionId: fr.GetSessionId(), Data: dataplane.AppendAnswer(nil, f.reason, "answer "+f.reason.String())}, time.Second)
		case <-ctx.Done():
			return
		}
	}
}

func (f *fakeServiceAgent) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opens
}

// TestResolveServiceRepicks (section 3 "Failover", the relay's half): an
// instance whose agent refuses (its target refused or unreachable) is not
// the answer: the relay picks another this open has not tried before it
// answers, and answers the last refusal once every instance refused; a
// refusal that would be the same on every instance is answered at once.
func TestResolveServiceRepicks(t *testing.T) {
	const (
		accept      = relayv1.ErrorReason_ERROR_REASON_UNSPECIFIED
		refused     = relayv1.ErrorReason_ERROR_REASON_TARGET_REFUSED
		unreachable = relayv1.ErrorReason_ERROR_REASON_TARGET_UNREACHABLE
		unauth      = relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED
	)
	for _, tc := range []struct {
		name       string
		a, b       relayv1.ErrorReason
		wantAgent  string // "a", "b"; "" refused
		wantReason []relayv1.ErrorReason
		wantOpens  int
	}{
		{"one refuses, the other takes it", refused, accept, "b", nil, -1},
		{"one's target unreachable, the other takes it", unreachable, accept, "b", nil, -1},
		{"every one refuses: the last refusal", refused, unreachable, "", []relayv1.ErrorReason{refused, unreachable}, 2},
		{"the same on every instance: at once", unauth, unauth, "", []relayv1.ErrorReason{unauth}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newServiceDispatcher(t, AccountIsOneGroup{})
			td := NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
			d.TCPDispatcher = td
			agents := map[string]*fakeServiceAgent{}
			for seed, reason := range map[string]relayv1.ErrorReason{"a": tc.a, "b": tc.b} {
				ref := publish(t, d, seed, serviceOwner, pgAtDB, nil)
				as := NewTCPAgentStream()
				as.answers = true
				td.Mu.Lock()
				td.Streams[ref] = as
				td.Mu.Unlock()
				f := &fakeServiceAgent{as: as, reason: reason}
				agents[seed] = f
				ctx, cancel := context.WithCancel(t.Context())
				t.Cleanup(cancel)
				go f.run(ctx)
			}
			c := &Core{Dispatcher: d, TCPDispatcher: td, Reach: d.KeyReach(func() TargetRules { return TargetRules{} }),
				DialPolicy: func() (int, time.Duration) { return 1, time.Millisecond }}
			caller := machinePrincipal("caller", serviceOwner)
			for range 8 { // both orders of the pick, likely
				before := agents["a"].count() + agents["b"].count()
				out, err := c.Open(t.Context(), caller, Target{Agent: "pg@db", E2EE: true, Answer: true, Hold: true})
				opens := agents["a"].count() + agents["b"].count() - before
				if tc.wantAgent != "" {
					if err != nil || out.AgentID != testRef(tc.wantAgent, "pgsrv") {
						t.Fatalf("opened %q, %v; want %s's", out.AgentID, err, tc.wantAgent)
					}
					// Forward reads the answer again: the same, not a second read.
					ctx, cancel := context.WithTimeout(t.Context(), time.Second)
					again := out.Session.(peerlink.ChainSession).Answered(ctx)
					cancel()
					if again != nil {
						t.Fatalf("the answer read again: %v", again)
					}
					_ = out.Close()
					continue
				}
				var a *link.Abort
				if !errors.As(err, &a) || !slices.Contains(tc.wantReason, answerReason(a)) {
					t.Fatalf("open: %v; want a refusal of %v", err, tc.wantReason)
				}
				if opens != tc.wantOpens {
					t.Fatalf("%d instances opened, want %d", opens, tc.wantOpens)
				}
			}
		})
	}
}

// TestPresenceServices: an agent's AgentUp carries its declaration, each
// service's health from its labels.
func TestPresenceServices(t *testing.T) {
	decls, err := agentservices.Parse("web=http,pg=tcp+e2ee;group=db;direct=on,ssh=tcp+plain")
	if err != nil {
		t.Fatal(err)
	}
	got := PresenceServices(decls, pgDown)
	want := []*presencev1.Service{
		{Name: "web", Kind: "http"},
		{Name: "pg", Kind: "tcp", E2Ee: "e2ee", Group: "db", Direct: "on", Down: true},
		{Name: "ssh", Kind: "tcp", E2Ee: "plain"},
	}
	if !slices.EqualFunc(got, want, func(a, b *presencev1.Service) bool { return proto.Equal(a, b) }) {
		t.Fatalf("services %v, want %v", got, want)
	}
	if PresenceServices(nil, nil) != nil {
		t.Fatal("no declaration lists services")
	}

	// The feed's AgentUp carries them.
	var f presenceFeed
	base := f.Watch(t.Context(), "", time.Second, nil)
	f.up("a", nil, nil, got)
	resp := f.Watch(t.Context(), base.GetCursor(), time.Second, nil)
	if evs := resp.GetEvents(); len(evs) != 1 || len(evs[0].GetUp().GetServices()) != 3 {
		t.Fatalf("watch %v: want the up with its 3 services", resp)
	}
}

// TestPresenceUpReadsCurrentHealth: the up of an agent's HTTP connection
// lists its services' health from its current labels (the ones the relay's
// pick reads), not the labels that connection carried: health set by
// UpdateLabels before the up is never stale in it.
func TestPresenceUpReadsCurrentHealth(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	stream := NewGRPCStream("svc-agent", nil, nil, nil, nil)
	d.Upstreams.Store("svc-agent", stream)
	if _, ok := d.UpdateAgentLabels("svc-agent", serialOf(d, "svc-agent"), pgDown, true); !ok {
		t.Fatal("UpdateAgentLabels: agent not found")
	}
	base := d.Presence.Watch(t.Context(), "", 0, nil).GetCursor()
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "svc-agent"))
	ctx = metadata.AppendToOutgoingContext(ctx, agentservices.Header, pgAtDB, agentservices.ModesHeader, "1")
	hs, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hs.Header(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		for _, ev := range d.Presence.Watch(t.Context(), base, time.Second, nil).GetEvents() {
			if svcs := ev.GetUp().GetServices(); len(svcs) > 0 {
				if len(svcs) != 1 || svcs[0].GetName() != "pg" || !svcs[0].GetDown() {
					t.Fatalf("up services %v, want pg down", svcs)
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no up event with services")
		}
	}
}

// TestResolveDatagramTarget (nfltr docs/decisions/nf1-udp-2026-11-14.md
// section 2): a udp service's session target (dns+udp) routes to the
// instances publishing that name as udp, by name or by the agent's ref; a
// name alone is its stream service; an agent without the udp service
// answers UNKNOWN_SERVICE.
func TestResolveDatagramTarget(t *testing.T) {
	d := newServiceDispatcher(t, AccountIsOneGroup{})
	d.TCPDispatcher = NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil)
	both := publish(t, d, "a", serviceOwner, "dns=tcp+e2ee;group=g,dns=udp+e2ee;group=g", nil)
	tcpOnly := publish(t, d, "b", serviceOwner, "dns=tcp+e2ee;group=g", nil)
	caller := machinePrincipal("caller", serviceOwner)
	resolve := func(agent, service string) (string, string, error) {
		return d.ResolveTarget(t.Context(), caller, d.DialReaches(caller), Target{Agent: agent, Service: service, E2EE: true}, TargetRules{})
	}
	seen := map[string]bool{}
	for range 40 {
		got, addr, err := resolve("dns@g", "dns+udp")
		if err != nil || got != both || addr != "dns+udp" {
			t.Fatalf("dns@g udp: %q %q %v; want %s's dns+udp", got, addr, err, both)
		}
		got, addr, err = resolve("dns@g", "")
		if err != nil || addr != "dns" {
			t.Fatalf("dns@g: %q %q %v; want dns", got, addr, err)
		}
		seen[got] = true
	}
	if !seen[both] || !seen[tcpOnly] {
		t.Fatalf("dns@g stream picked %v; want both instances", seen)
	}
	if got, addr, err := resolve(both, "dns+udp"); err != nil || got != both || addr != "dns+udp" {
		t.Fatalf("by ref: %q %q %v", got, addr, err)
	}
	var r *Refusal
	if _, _, err := resolve(tcpOnly, "dns+udp"); !errors.As(err, &r) || r.Reason != relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE {
		t.Fatalf("tcp-only agent's dns+udp: %v, want UNKNOWN_SERVICE", err)
	}
	if _, _, err := resolve("dns@g", "web+udp"); !errors.As(err, &r) || r.Reason != relayv1.ErrorReason_ERROR_REASON_INVALID_ARGUMENT {
		t.Fatalf("another udp service in the target: %v, want INVALID_ARGUMENT", err)
	}
}
