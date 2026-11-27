// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package nf1cap is the owner's daily cap on an nf1 address's relayed
// bytes (nfltr docs/decisions/nf1-relay-cap-2026-11-25.md,
// spec/NF1Cap.tla): every byte the relay carries on a relayed leg of a
// session whose client is not the address owner's own account counts
// against the address (its rid) for the relay's UTC day; past the owner's
// cap (the serving agent's Label of the service, else Settings.Default) the relay refuses
// new opens and cuts open legs. Per replica and in memory, as every nf1
// cap.
package nf1cap

import (
	"strconv"
	"sync"
	"time"

	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/spf13/viper"
)

// LabelPrefix starts the serving agent's label that sets one address's
// cap: LabelPrefix + the service's session target (agentservices
// Decl.Target, as the agent's other per-service labels), bytes a UTC day,
// decimal (nfltr cat serve --relay-cap).
const LabelPrefix = "nfltr.nf1.relay_cap."

// Label is the label of the service whose session target is target.
func Label(target string) string { return LabelPrefix + target }

// Reason is the refusal's and the cut's reason, at both ends.
const Reason = relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED

// Msg is the refusal's and the cut's message.
const Msg = "nf1: this address reached its owner's daily cap on relayed bytes for clients outside the owner's account (resets 00:00 UTC)"

// Settings are the nf1.relay_cap* settings (platform/config).
type Settings struct {
	Default int64         // an address's cap when its agent sets none
	Refresh time.Duration // how often an open leg's write re-reads the cap
}

// FromSettings reads them.
func FromSettings() Settings {
	return Settings{Default: viper.GetInt64("nf1.relay_cap"), Refresh: viper.GetDuration("nf1.relay_cap_refresh")}
}

// CapOf is the cap the label key of an agent with labels sets: def when
// it sets none, 0 when its value is no count of bytes (fail closed, never
// the default).
func CapOf(labels map[string]string, key string, def int64) int64 {
	v, ok := labels[key]
	if !ok {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

type rid = [agentservices.RIDSize]byte

// address is one rid's count today and its cap.
type address struct {
	day        int64 // the UTC day the count is of
	used       int64
	cap        int64
	capAt      time.Time
	capOf      func() int64 // re-reads the cap: the last open's agent
	refreshing bool
	legs       int // legs opened, for the trace
}

// Registry counts the addresses' relayed bytes on one replica. Safe for
// concurrent use.
// ponytail: one lock for every address, per-address locks if relayed
// writes ever contend on it.
type Registry struct {
	s   Settings
	now func() time.Time

	mu    sync.Mutex
	addrs map[rid]*address

	tr tracer // -tags relaytrace: spec/NF1Cap.tla's steps (trace_on.go)
}

// New is a registry with s.
func New(s Settings) *Registry {
	return &Registry{s: s, now: time.Now, addrs: map[rid]*address{}}
}

func dayOf(t time.Time) int64 { return t.UTC().Unix() / 86400 }

// get is r's entry for id today, under r.mu. An entry of a past day counts
// nothing: forgetting it loses nothing, so they are swept as the map grows.
func (r *Registry) get(id rid, now time.Time) *address {
	day := dayOf(now)
	a := r.addrs[id]
	if a == nil {
		if len(r.addrs) >= 1024 && len(r.addrs)&(len(r.addrs)-1) == 0 { // at each power of two
			for k, v := range r.addrs {
				if v.day != day {
					delete(r.addrs, k)
				}
			}
		}
		a = &address{day: day, cap: r.s.Default, capAt: now}
		r.addrs[id] = a
	}
	if a.day != day { // NewDay
		a.day, a.used = day, 0
		r.trace(id, event{Ev: "NewDay"})
	}
	return a
}

// learn sets a's cap to c (NF1Cap.tla Learn), under r.mu.
func (r *Registry) learn(id rid, a *address, c int64, now time.Time) {
	if c != a.cap {
		r.trace(id, event{Ev: "Learn", Cap: c})
	}
	a.cap, a.capAt = c, now
}

// Admit opens a counted leg of id: it takes cap, the cap the agent the
// open resolved to sets (Learn), and capOf to re-read it later; nil when
// the address is at its cap today (the open is refused: Open).
func (r *Registry) Admit(id rid, cap int64, capOf func() int64) *Leg {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.get(id, now)
	r.learn(id, a, cap, now)
	a.capOf = capOf
	a.legs++
	ok := a.used < a.cap
	r.trace(id, event{Ev: "Open", S: a.legs, OK: ok})
	if !ok {
		metrics.NF1CapRefused(metrics.NF1CapAtOpen)
		return nil
	}
	return &Leg{r: r, id: id, s: a.legs}
}

// Leg is one counted relayed leg of an address: a session's framed or
// control session, or a re-attach (NF1Cap.tla's session s).
type Leg struct {
	r     *Registry
	id    rid
	s     int
	ended bool // under r.mu
}

// Take counts n bytes the leg is about to carry, both ways: how many of
// them fit today (Relay). Fewer than n is the cut (Cut): the leg carries
// nothing more. Past the refresh interval it re-reads the cap in the
// background.
func (l *Leg) Take(n int) int {
	r := l.r
	now := r.now()
	r.mu.Lock()
	a := r.get(l.id, now)
	fit := int64(0)
	if !l.ended {
		fit = min(int64(n), max(a.cap-a.used, 0))
	}
	a.used += fit
	if fit > 0 {
		r.trace(l.id, event{Ev: "Relay", S: l.s, N: fit, U: a.used})
	}
	cut := fit < int64(n) && !l.ended
	if cut {
		l.ended = true
		r.trace(l.id, event{Ev: "Cut", S: l.s})
	}
	if a.capOf != nil && !a.refreshing && now.Sub(a.capAt) >= r.s.Refresh {
		a.refreshing = true
		go r.refresh(l.id, a.capOf)
	}
	r.mu.Unlock()
	metrics.NF1CapRelayed(int(fit))
	if cut {
		metrics.NF1CapRefused(metrics.NF1CapCut)
	}
	return int(fit)
}

// End notes the leg ended otherwise (its peers ended it, a punch released
// it); once.
func (l *Leg) End() {
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	if !l.ended {
		l.ended = true
		l.r.trace(l.id, event{Ev: "End", S: l.s})
	}
}

// refresh re-reads id's cap with capOf (Learn: the agent's record on
// another replica, its stream here).
func (r *Registry) refresh(id rid, capOf func() int64) {
	c := capOf()
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	a := r.get(id, now)
	r.learn(id, a, c, now)
	a.refreshing = false
}

// Usage is id's relayed bytes today and its cap on this replica (the
// default for an address not opened today).
func (r *Registry) Usage(id rid) (used, cap int64) {
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.addrs[id]
	if !ok || a.day != dayOf(now) {
		if ok {
			return 0, a.cap
		}
		return 0, r.s.Default
	}
	return a.used, a.cap
}

// event is one line of a trace (-tags relaytrace, trace_on.go): ev the
// step, s the leg, n the bytes a Relay carried and u the address's count
// after it, cap a Learn's, ok an Open's admission.
type event struct {
	Ev  string `json:"ev"`
	S   int    `json:"s,omitempty"`
	N   int64  `json:"n,omitempty"`
	U   int64  `json:"u,omitempty"`
	Cap int64  `json:"cap,omitempty"`
	OK  bool   `json:"ok,omitempty"`
}
