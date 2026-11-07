// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connlimit

// An account's own limits, past the per-source floor above: once the relay
// knows whose connection it is (an E2EE share's agent's owner), the
// connection counts against that account in total and against (account,
// source address), open connections and new ones a second, under the
// limits of the account's plan. One account's visitors behind a NAT spend
// that account's budget only. spec/AccountLimit.tla (LimitHolds,
// NoAccountStarvesAnother, CountsMatch); nfltr/nfltr
// docs/decisions/relay-plan-conn-limits-2026-11-08.md.

import (
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/platform/ratelimit"
)

// Refusal reasons of an account's limits (metrics.IncrConnRefusals).
const (
	ReasonAccountConns       = "account_conns"
	ReasonAccountRate        = "account_rate"
	ReasonAccountSourceConns = "account_source_conns"
	ReasonAccountSourceRate  = "account_source_rate"
)

// Limits are a plan's connection limits; each 0: no limit.
type Limits struct {
	Max         int     // open connections of the account
	Rate        float64 // new connections a second of the account
	Burst       int     // Rate's burst
	SourceMax   int     // open connections of the account from one source address
	SourceRate  float64 // new connections a second of the account from one source address
	SourceBurst int     // SourceRate's burst
}

func (l Limits) none() bool {
	return l.Max <= 0 && l.Rate <= 0 && l.SourceMax <= 0 && l.SourceRate <= 0
}

// Accounts counts admitted connections per account and per (account,
// source address). The zero value is unusable: NewAccounts.
type Accounts struct {
	def  Limits
	plan atomic.Pointer[Limits] // the plan's limits; nil: def

	mu    sync.Mutex
	open  map[string]int // admitted connections not yet closed, by account
	sopen map[srcKey]int // the same by (account, source address)
	rate  *ratelimit.Limiter
	srate *ratelimit.Limiter
}

type srcKey struct {
	account string
	ip      netip.Addr
}

// NewAccounts is a limiter with def as the limits while no plan is set
// (an own relay's flags).
func NewAccounts(def Limits) *Accounts {
	// ponytail: an idle bucket is forgotten after 5 min (refilled by then
	// unless a plan's burst takes longer to refill); 100k keys each.
	return &Accounts{
		def: def, open: map[string]int{}, sopen: map[srcKey]int{},
		rate:  ratelimit.New(0, 0, time.Minute, 5*time.Minute),
		srate: ratelimit.New(0, 0, time.Minute, 5*time.Minute),
	}
}

// SetPlan makes l the limits of every account from the next admission
// on (nil: the default); connections open over a lowered limit stay.
func (a *Accounts) SetPlan(l *Limits) { a.plan.Store(l) }

// Limits are the limits in force.
func (a *Accounts) Limits() Limits {
	if p := a.plan.Load(); p != nil {
		return *p
	}
	return a.def
}

func nop() {}

// Admit counts a connection of account from ip: release frees its counts
// (once, however often called), or reason says which limit refused it
// (release then is nil). No account (""), no address or no limit admits.
func (a *Accounts) Admit(account string, ip net.IP) (release func(), reason string) {
	if a == nil {
		return nop, ""
	}
	l := a.Limits()
	addr, ok := netip.AddrFromSlice(ip)
	if l.none() || account == "" || !ok {
		return nop, ""
	}
	k := srcKey{account, addr.Unmap()}
	var src string // the source bucket's key
	if l.SourceRate > 0 {
		src = account + " " + k.ip.String()
	}
	a.mu.Lock()
	switch {
	case l.Max > 0 && a.open[account] >= l.Max:
		reason = ReasonAccountConns
	case l.SourceMax > 0 && a.sopen[k] >= l.SourceMax:
		reason = ReasonAccountSourceConns
	case l.Rate > 0 && !a.rate.Ready(account, l.Rate, max(l.Burst, 1)):
		reason = ReasonAccountRate
	case l.SourceRate > 0 && !a.srate.Ready(src, l.SourceRate, max(l.SourceBurst, 1)):
		reason = ReasonAccountSourceRate
	default:
		// Both buckets checked under a.mu before either is spent.
		if l.Rate > 0 {
			a.rate.Allow(account)
		}
		if l.SourceRate > 0 {
			a.srate.Allow(src)
		}
		a.open[account]++
		a.sopen[k]++
	}
	a.mu.Unlock()
	if reason != "" {
		return nil, reason
	}
	return sync.OnceFunc(func() {
		a.mu.Lock()
		if a.open[account]--; a.open[account] <= 0 {
			delete(a.open, account)
		}
		if a.sopen[k]--; a.sopen[k] <= 0 {
			delete(a.sopen, k)
		}
		a.mu.Unlock()
	}), ""
}
