// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package nf1anon is an nf1 address served with no account (nfltr
// docs/decisions/nf1-anon-2026-11-16.md, spec/NF1Anon.tla): an agent that
// connects with no credential and declares only capability addresses,
// each with its registration proof, is an anonymous principal (provider
// Provider, no owner, agent id AgentID(rid), NotAfter its lifetime). Only
// an nf1 open reaches it (IsAgent), and every relayed leg of its sessions
// is budgeted (Registry.Leg): the relay carries such a session's
// signalling, never its bulk.
package nf1anon

import (
	"encoding/hex"
	"errors"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/relaystatus"
	"github.com/nfltr/relay-core/platform/connlimit"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/spf13/viper"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the anonymous principal's provider: no provider chain
// states it (it holds no ':' and no account is behind it).
const Provider = "nf1anon"

// agentPrefix starts an anonymous agent's id: ':' is in no account's agent
// id (identity.AgentRef, ValidService), so no account can take one.
const agentPrefix = "nf1:"

// AgentID is the anonymous agent's id for the first rid it declares.
func AgentID(rid [agentservices.RIDSize]byte) string {
	return agentPrefix + hex.EncodeToString(rid[:])
}

// IsAgent reports whether agentID is an anonymous agent's: no route but an
// nf1 open reaches it (spec/NF1Anon.tla Open, mutant keyed-route).
func IsAgent(agentID string) bool { return strings.HasPrefix(agentID, agentPrefix) }

// Reason is the cut's reason, at both ends (relay proto 52).
const Reason = relayv1.ErrorReason_ERROR_REASON_ACCOUNT_REQUIRED

// why is Reason as a trace names it (spec/NF1Anon.tla CutWhy).
var why = strings.ToLower(strings.TrimPrefix(Reason.String(), "ERROR_REASON_"))

// Msg is the cut's message, at both ends.
const Msg = "nf1: no direct path formed, and this address has no account behind it: relaying needs the serving side's nfltr account"

// Settings are the nf1.anon.* settings (relay-core platform/config).
type Settings struct {
	Enabled     bool
	Lifetime    time.Duration // a registration's NotAfter after its start
	LegBytes    int64         // bytes one relayed leg carries, both ways
	LegTime     time.Duration // how long one relayed leg lives
	BucketBytes int           // the source's relayed-byte bucket
	BucketRate  float64       // its refill, bytes a second
	Max         int           // anonymous registrations open on this replica
	SourceMax   int           // anonymous registrations open from one source
	SourceRate  float64       // new anonymous registrations a second from one source
	SourceBurst int           // SourceRate's burst
}

// ErrNotAnonymous: the connection is no anonymous registration (it
// declares nothing, or something besides capability addresses): it is
// refused as any connection with no credential is.
var ErrNotAnonymous = errors.New("nf1anon: not an anonymous registration")

// Registry admits anonymous registrations on one replica and budgets
// their sessions' relayed legs. Safe for concurrent use.
type Registry struct {
	s     Settings
	hosts []string
	regs  *connlimit.Accounts
	now   func() time.Time

	mu      sync.Mutex
	src     map[string]reg               // the agents registered here
	buckets map[netip.Addr]*rate.Limiter // a source's relayed bytes, across its registrations

	tr tracer // -tags relaytrace: spec/NF1Anon.tla's steps (trace_on.go)
}

// reg is a registration's source and NotAfter.
type reg struct {
	src      netip.Addr
	notAfter time.Time
}

func nop() {}

// account keys every anonymous registration in regs: one account, its
// sources (IPv4, IPv6 /64) counted apart.
const account = "nf1anon"

// New is a registry with s for the relay hosts hosts (nf1.hosts).
func New(s Settings, hosts []string) *Registry {
	return &Registry{
		s: s, hosts: hosts, now: time.Now,
		regs: connlimit.NewAccounts(connlimit.Limits{Max: s.Max, SourceMax: s.SourceMax,
			SourceRate: s.SourceRate, SourceBurst: s.SourceBurst}),
		src: map[string]reg{}, buckets: map[netip.Addr]*rate.Limiter{},
	}
}

// Enabled reports whether r takes anonymous registrations.
func (r *Registry) Enabled() bool { return r != nil && r.s.Enabled }

// Register checks a connection with no credential from ip that declares
// decl (agentservices.Header, "" none) with the registration proofs regs
// (agentservices.RegHeader): its anonymous principal and the release of
// its registration (once, when the connection ends), ErrNotAnonymous, or
// a gRPC status: InvalidArgument for a proof that does not hold,
// ResourceExhausted (ERROR_REASON_LIMIT_EXCEEDED) over a cap. register
// is the agent's registering stream (ConnectHTTP), which counts against
// the caps; another of its streams (ConnectTCP) joins the registration
// its agent holds here, else it is ErrNotAnonymous.
func (r *Registry) Register(decl string, regs []string, ip net.IP, register bool) (identity.Principal, func(), error) {
	if !r.Enabled() || decl == "" {
		return identity.Principal{}, nil, ErrNotAnonymous
	}
	decls, err := agentservices.Parse(decl)
	if err != nil || len(decls) == 0 || slices.ContainsFunc(decls, func(d agentservices.Decl) bool { return d.NF1 == [agentservices.RIDSize]byte{} }) {
		return identity.Principal{}, nil, ErrNotAnonymous // a plain service, a group, HTTP: an account's
	}
	proofs, err := agentservices.ParseReg(regs)
	if err != nil {
		return identity.Principal{}, nil, status.Errorf(codes.InvalidArgument, "%s: malformed", agentservices.RegHeader)
	}
	for _, d := range decls {
		p, ok := proofs[d.NF1]
		if !ok || !slices.ContainsFunc(r.hosts, func(h string) bool { return agentservices.CheckRegistration(h, d.NF1, p) == nil }) {
			metrics.NF1AnonRegistration(metrics.NF1AnonRefusedProof)
			return identity.Principal{}, nil, status.Errorf(codes.InvalidArgument, "service %s: an address served with no account needs its registration proof for this relay (%s)", d.Name, agentservices.RegHeader)
		}
	}
	src, id := Source(ip), AgentID(decls[0].NF1)
	p := identity.Principal{Provider: Provider, Subject: id[len(agentPrefix):], AgentID: id, NotAfter: r.now().Add(r.s.Lifetime)}
	if !register {
		r.mu.Lock()
		g, ok := r.src[id]
		r.mu.Unlock()
		if !ok {
			return identity.Principal{}, nil, ErrNotAnonymous
		}
		p.NotAfter = g.notAfter
		return p, nop, nil
	}
	release, reason := r.regs.Admit(account, src.AsSlice())
	if reason != "" {
		metrics.NF1AnonRegistration(metrics.NF1AnonRefusedLimit)
		return identity.Principal{}, nil, relaystatus.New(codes.ResourceExhausted, relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "nf1: over the relay's limit on registrations with no account ("+reason+")").Err()
	}
	g := reg{src, p.NotAfter}
	r.mu.Lock()
	old, had := r.src[id]
	r.src[id] = g
	r.mu.Unlock()
	if had { // its holder's next connection takes the registration over
		r.traceStep(old.src, "Leave")
	}
	r.traceStep(src, "Register")
	metrics.NF1AnonRegistration(metrics.NF1AnonRegistered)
	return p, sync.OnceFunc(func() {
		metrics.NF1AnonLeft()
		r.mu.Lock()
		left := r.src[id] == g
		if left {
			delete(r.src, id)
		}
		r.mu.Unlock()
		if left { // not Expired
			r.traceStep(src, "Leave")
		}
		release()
	}), nil
}

// Expired notes that agentID's registration reached its NotAfter (the
// relay ended its connection: NF1Anon.tla Expire); its release follows.
func (r *Registry) Expired(agentID string) {
	r.mu.Lock()
	g, ok := r.src[agentID]
	delete(r.src, agentID)
	r.mu.Unlock()
	if ok {
		r.traceStep(g.src, "Expire")
		metrics.NF1AnonRegistration(metrics.NF1AnonExpired)
	}
}

// SourceOf is the source (an IPv4 address, an IPv6 /64) agentID's
// anonymous registration came from, while it is registered here: what an
// audit line names instead of the agent's owner, which it has none of.
func (r *Registry) SourceOf(agentID string) (netip.Addr, bool) {
	if r == nil {
		return netip.Addr{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.src[agentID]
	return g.src, ok
}

// traceStep logs a registration's step ev for src.
func (r *Registry) traceStep(src netip.Addr, ev string) {
	defer r.traceLock()()
	r.trace(src, event{Ev: ev}, 0)
}

// event is one line of a trace (-tags relaytrace, trace_on.go): ev the
// step, s the leg, srv its server, n the bytes a Relay carried, why a
// cut's reason.
type event struct {
	Ev  string `json:"ev"`
	S   int    `json:"s,omitempty"`
	Srv string `json:"srv,omitempty"`
	N   int    `json:"n,omitempty"`
	Why string `json:"why,omitempty"`
}

// Source is ip's source key: an IPv4 address, an IPv6 /64.
func Source(ip net.IP) netip.Addr {
	a, _ := netip.AddrFromSlice(ip)
	return connlimit.SourceKey(a)
}

// bucket is the relayed-byte bucket of the agent's source while it is
// registered here, and that source; of the agent itself when its
// registration is another replica's (per replica, as every nf1 cap).
func (r *Registry) bucket(agentID string) (netip.Addr, *rate.Limiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.src[agentID]
	key := g.src
	if !ok {
		var rid [agentservices.RIDSize]byte
		_, _ = hex.Decode(rid[:], []byte(agentID[len(agentPrefix):]))
		key = netip.AddrFrom16(rid) // ponytail: no source known here: the rid's own bucket, which a new registration of another key does not share
	}
	b := r.buckets[key]
	if b == nil {
		if len(r.buckets) > 4*max(r.s.Max, 1) { // forget the full ones: refilled, nothing to remember
			now := r.now()
			for k, v := range r.buckets {
				if v.TokensAt(now) >= float64(v.Burst()) {
					delete(r.buckets, k)
				}
			}
		}
		b = rate.NewLimiter(rate.Limit(r.s.BucketRate), r.s.BucketBytes)
		r.buckets[key] = b
	}
	return key, b
}

// Leg is the budget of one relayed leg of a session to agentID, nil when
// agentID is no anonymous agent (an account's session is never budgeted:
// NF1Anon.tla NoCutOnAccount).
func (r *Registry) Leg(agentID string) *Leg {
	if r == nil || !IsAgent(agentID) {
		return nil
	}
	src, b := r.bucket(agentID)
	l := &Leg{r: r, src: src, b: b, left: r.s.LegBytes, Time: r.s.LegTime}
	defer r.traceLock()()
	l.id = r.trace(src, event{Ev: "Open", Srv: "anon"}, 0)
	return l
}

// Leg counts one relayed leg's bytes, both ways, against the leg's budget
// and its source's bucket (NF1Anon.tla Relay, Cut, TimeCut).
type Leg struct {
	r     *Registry
	src   netip.Addr
	b     *rate.Limiter
	id    int
	mu    sync.Mutex
	left  int64
	ended bool
	Time  time.Duration // the leg's time limit
}

// Take counts n bytes the leg is about to carry: how many of them fit.
// Fewer than n is the cut (Relay of what fits, then Cut): the leg carries
// nothing more.
func (l *Leg) Take(n int) int {
	defer l.r.traceLock()()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended {
		return 0
	}
	now := l.r.now()
	fit := min(int64(n), l.left, int64(max(l.b.TokensAt(now), 0)))
	if fit > 0 {
		l.b.AllowN(now, int(fit))
		l.left -= fit
		l.r.trace(l.src, event{Ev: "Relay", S: l.id, N: int(fit)}, l.id)
		metrics.NF1AnonRelayed(int(fit))
	}
	if fit < int64(n) {
		l.ended = true
		metrics.NF1AnonCut(metrics.NF1AnonCutBytes)
		l.r.trace(l.src, event{Ev: "Cut", S: l.id, Why: why}, l.id)
	}
	return int(fit)
}

// TimeUp ends the leg at its time limit (TimeCut): false when it had
// ended already.
func (l *Leg) TimeUp() bool {
	if !l.end(event{Ev: "TimeCut", Why: why}) {
		return false
	}
	metrics.NF1AnonCut(metrics.NF1AnonCutTime)
	return true
}

// End notes the leg ended otherwise (its peers ended it, a punch released
// it): false when it had ended already.
func (l *Leg) End() bool { return l.end(event{Ev: "End"}) }

func (l *Leg) end(e event) bool {
	defer l.r.traceLock()()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended {
		return false
	}
	l.ended = true
	e.S = l.id
	l.r.trace(l.src, e, l.id)
	return true
}

// FromSettings reads the nf1.anon.* settings (platform/config's flags).
func FromSettings() Settings {
	return Settings{
		Enabled:     viper.GetBool("nf1.anon.enabled"),
		Lifetime:    viper.GetDuration("nf1.anon.lifetime"),
		LegBytes:    viper.GetInt64("nf1.anon.leg_bytes"),
		LegTime:     viper.GetDuration("nf1.anon.leg_time"),
		BucketBytes: viper.GetInt("nf1.anon.bucket_bytes"),
		BucketRate:  viper.GetFloat64("nf1.anon.bucket_rate"),
		Max:         viper.GetInt("nf1.anon.max"),
		SourceMax:   viper.GetInt("nf1.anon.source_max"),
		SourceRate:  viper.GetFloat64("nf1.anon.source_rate"),
		SourceBurst: viper.GetInt("nf1.anon.source_burst"),
	}
}
