// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connlimit

import (
	"fmt"
	"net"
	"testing"
)

var nat = net.ParseIP("203.0.113.7")

// Open connections: per account and per (account, source); a release
// frees its slot once (spec CountsMatch, mutant release-twice).
func TestAccountsOpenLimits(t *testing.T) {
	a := NewAccounts(Limits{Max: 3, SourceMax: 2})
	r1, _ := a.Admit("acct", nat)
	r2, _ := a.Admit("acct", nat)
	if _, why := a.Admit("acct", nat); why != ReasonAccountSourceConns {
		t.Fatalf("third from one source: %q, want %s", why, ReasonAccountSourceConns)
	}
	r3, why := a.Admit("acct", net.ParseIP("198.51.100.1"))
	if why != "" {
		t.Fatalf("another source: %q", why)
	}
	if _, why := a.Admit("acct", net.ParseIP("198.51.100.2")); why != ReasonAccountConns {
		t.Fatalf("fourth of the account: %q, want %s", why, ReasonAccountConns)
	}
	r1()
	r1() // a second Close frees nothing more
	if _, why := a.Admit("acct", nat); why != "" {
		t.Fatalf("after a release: %q", why)
	}
	if _, why := a.Admit("acct", nat); why != ReasonAccountConns {
		t.Fatalf("a double release freed a second slot: %q", why)
	}
	r2()
	r3()
}

// Two accounts' visitors behind one address have a budget each (spec
// NoAccountStarvesAnother, mutant count-by-ip-only).
func TestAccountsNoAccountStarvesAnother(t *testing.T) {
	a := NewAccounts(Limits{SourceMax: 1, SourceRate: 1, SourceBurst: 1})
	if _, why := a.Admit("one", nat); why != "" {
		t.Fatal(why)
	}
	if _, why := a.Admit("one", nat); why == "" {
		t.Fatal("one over its own limit was admitted")
	}
	if _, why := a.Admit("two", nat); why != "" {
		t.Fatalf("two refused for one's connections: %s", why)
	}
}

// New connections a second, and a plan lowered under buckets already
// made reaches them (spec LimitHolds, mutant cache-stale-plan).
func TestAccountsPlanReachesBuckets(t *testing.T) {
	a := NewAccounts(Limits{Rate: 0.001, Burst: 5})
	for i := range 2 {
		r, why := a.Admit("acct", nat)
		if why != "" {
			t.Fatalf("admission %d: %s", i, why)
		}
		r()
	}
	a.SetPlan(&Limits{Rate: 0.001, Burst: 1})
	r, why := a.Admit("acct", nat)
	if why != "" {
		t.Fatalf("first under the lowered plan: %s", why)
	}
	r()
	if _, why := a.Admit("acct", nat); why != ReasonAccountRate {
		t.Fatalf("second under a plan of burst 1: %q, want %s (the bucket kept the old burst)", why, ReasonAccountRate)
	}
	a.SetPlan(nil)
	if got := a.Limits(); got.Burst != 5 {
		t.Fatalf("SetPlan(nil): %+v, want the default", got)
	}
}

// A refusal by the source bucket spends no token of the account's.
func TestAccountsRefusalSpendsNothing(t *testing.T) {
	a := NewAccounts(Limits{Rate: 0.001, Burst: 2, SourceRate: 0.001, SourceBurst: 1})
	if _, why := a.Admit("acct", nat); why != "" {
		t.Fatal(why)
	}
	if _, why := a.Admit("acct", nat); why != ReasonAccountSourceRate {
		t.Fatalf("got %q, want %s", why, ReasonAccountSourceRate)
	}
	if _, why := a.Admit("acct", net.ParseIP("198.51.100.1")); why != "" {
		t.Fatalf("the refused one spent the account's token: %s", why)
	}
}

func TestAccountsWithoutLimitsAdmit(t *testing.T) {
	var none *Accounts
	if r, why := none.Admit("acct", nat); why != "" || r == nil {
		t.Fatal("nil limiter refused")
	}
	a := NewAccounts(Limits{})
	if r, why := a.Admit("acct", nat); why != "" || r == nil {
		t.Fatal("no limits refused")
	}
	a.SetPlan(&Limits{Max: 1})
	if _, why := a.Admit("", nat); why != "" {
		t.Fatal("no account refused")
	}
}

// The TENANTS scale: 10,000 accounts with visitors from 100,000 addresses.
func BenchmarkAccountsAdmit(b *testing.B) {
	a := NewAccounts(Limits{Max: 1 << 30, Rate: 1e9, Burst: 1 << 30, SourceMax: 1 << 30, SourceRate: 1e9, SourceBurst: 1 << 30})
	accts := make([]string, 10_000)
	for i := range accts {
		accts[i] = fmt.Sprintf("acct-%d@example.com", i)
	}
	ips := make([]net.IP, 100_000)
	for i := range ips {
		ips[i] = net.IPv4(10, byte(i>>16), byte(i>>8), byte(i))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, why := a.Admit(accts[i%len(accts)], ips[i%len(ips)])
		if why != "" {
			b.Fatal(why)
		}
		r()
	}
}

// No plan: what every connection pays on a relay without account limits.
func BenchmarkAccountsAdmitNoLimits(b *testing.B) {
	a := NewAccounts(Limits{})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, _ := a.Admit("acct", nat)
		r()
	}
}
