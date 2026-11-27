// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package nf1cap

import (
	"sync/atomic"
	"testing"
	"time"
)

var ridA, ridB = [16]byte{1}, [16]byte{2}

// clock is a registry's settable time.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newTest(def int64, refresh time.Duration) (*Registry, *clock) {
	c := &clock{t: time.Date(2026, 11, 25, 23, 0, 0, 0, time.UTC)}
	r := New(Settings{Default: def, Refresh: refresh})
	r.now = c.now
	return r, c
}

// TestCapOf: the label is the cap in bytes a day; missing is the default,
// unparsable or negative is 0 (fail closed: never the default).
func TestCapOf(t *testing.T) {
	for _, c := range []struct {
		labels map[string]string
		want   int64
	}{
		{nil, 100},
		{map[string]string{Label("clip"): "5"}, 5},
		{map[string]string{Label("clip"): "0"}, 0},
		{map[string]string{Label("clip"): "4GiB"}, 0},
		{map[string]string{Label("clip"): "-1"}, 0},
		{map[string]string{Label("other"): "1"}, 100},
	} {
		if got := CapOf(c.labels, Label("clip"), 100); got != c.want {
			t.Errorf("CapOf(%v) = %d, want %d", c.labels, got, c.want)
		}
	}
}

// TestTakeCountsOncePerAddress (spec/NF1Cap.tla CountedOnce, CapBounded,
// mutant reset-on-reconnect): every leg of an address counts against one
// counter per rid, whatever agent or session it comes from; what fits is
// carried and nothing past the cap; another rid (--new-address) has its own.
func TestTakeCountsOncePerAddress(t *testing.T) {
	r, _ := newTest(10, time.Minute)
	l1 := r.Admit(ridA, 10, nil)
	if l1 == nil {
		t.Fatal("a fresh address refused")
	}
	if got := l1.Take(6); got != 6 {
		t.Fatalf("Take 6 = %d", got)
	}
	l2 := r.Admit(ridA, 10, nil) // another session, or the agent reconnected
	if l2 == nil {
		t.Fatal("refused under the cap")
	}
	if got := l2.Take(6); got != 4 {
		t.Fatalf("Take 6 at 6 of 10 = %d, want 4", got)
	}
	if got := l1.Take(1); got != 0 {
		t.Fatalf("Take past the cap = %d", got)
	}
	if r.Admit(ridA, 10, nil) != nil {
		t.Fatal("an open at the cap admitted")
	}
	if used, c := r.Usage(ridA); used != 10 || c != 10 {
		t.Fatalf("Usage = %d of %d", used, c)
	}
	if l := r.Admit(ridB, 10, nil); l == nil || l.Take(10) != 10 {
		t.Fatal("another address shares the count")
	}
}

// TestNewDay (NewDay): the UTC day's end starts the count again.
func TestNewDay(t *testing.T) {
	r, c := newTest(4, time.Minute)
	l := r.Admit(ridA, 4, nil)
	if l.Take(4) != 4 || l.Take(1) != 0 {
		t.Fatal("cap not held")
	}
	c.t = c.t.Add(time.Hour) // 00:00 UTC
	if l := r.Admit(ridA, 4, nil); l == nil || l.Take(4) != 4 {
		t.Fatal("the next day kept yesterday's count")
	}
}

// TestLoweredCapAppliesToOpenLegs (Learn, mutants stale-cap, no-learn): an
// open's cap applies to every open leg's next write; past the refresh
// interval a write re-reads the cap (another replica's agent) without
// blocking.
func TestLoweredCapAppliesToOpenLegs(t *testing.T) {
	r, c := newTest(10, time.Minute)
	var cap atomic.Int64
	cap.Store(10)
	read := make(chan struct{}, 4)
	capOf := func() int64 { read <- struct{}{}; return cap.Load() }
	open := r.Admit(ridA, 10, capOf)
	idle := r.Admit(ridA, 10, capOf) // a leg open before every change
	if open.Take(3) != 3 {
		t.Fatal("Take 3")
	}
	r.Admit(ridA, 4, capOf).End() // the agent's label lowered, read at another open
	if got := open.Take(3); got != 1 {
		t.Fatalf("Take 3 at 3 of a lowered 4 = %d, want 1", got)
	}
	cap.Store(6)                   // the label raised; no open reads it
	c.t = c.t.Add(2 * time.Minute) // past the refresh
	idle.Take(0)
	select {
	case <-read:
	case <-time.After(10 * time.Second):
		t.Fatal("no re-read past the refresh interval")
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		if _, got := r.Usage(ridA); got == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the re-read cap was not applied")
		}
	}
	if got := idle.Take(3); got != 2 {
		t.Fatalf("Take 3 at 4 of a re-read 6 = %d, want 2", got)
	}
}
