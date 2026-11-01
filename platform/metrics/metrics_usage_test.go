// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"io"
	"net"
	"testing"
	"time"
)

type usageKey struct {
	account, agentID string
	p                Path
}

// takeAll is TakeUsage's rows summed by payer and path.
func takeAll() map[usageKey][2]uint64 {
	out := map[usageKey][2]uint64{}
	TakeUsage(func(u Usage) {
		k := usageKey{u.Account, u.AgentID, u.Path}
		v := out[k]
		out[k] = [2]uint64{v[0] + u.ToAgent, v[1] + u.FromAgent}
	})
	return out
}

// pathSum is rows' bytes on p, both directions.
func pathSum(rows map[usageKey][2]uint64, p Path) (to, from uint64) {
	for k, v := range rows {
		if k.p == p {
			to, from = to+v[0], from+v[1]
		}
	}
	return to, from
}

// TestUsageConservesPathBytes: what TakeUsage returns for a frontend path
// over any number of takes is exactly what its transfer counter counted
// meanwhile: metered sessions under their payer, live across takes or
// ended between them, and bytes counted without a meter under no payer.
func TestUsageConservesPathBytes(t *testing.T) {
	EnableUsage()
	p := PathA2A
	takeAll() // what earlier tests counted
	before := scrape(t, p)

	a := OpenMeter(p, "owner-a", "agent-a")
	b := OpenMeter(p, "owner-b", "agent-b")
	AddBytes(p, a, 100, 1)
	AddBytes(p, b, 7, 0)
	AddTransferBytes(p, 3, 4) // no session's
	first := takeAll()

	AddBytes(p, a, 10, 2)
	AddBytes(p, b, 0, 9)
	b.End() // ended between takes: its last bytes still count, once
	AddBytes(p, a, 1, 1)
	second := takeAll()
	a.End()
	third := takeAll()

	want := map[usageKey][2]uint64{
		{"owner-a", "agent-a", p}: {111, 4},
		{"owner-b", "agent-b", p}: {7, 9},
		{"", "", p}:               {3, 4},
	}
	got := map[usageKey][2]uint64{}
	for _, rows := range []map[usageKey][2]uint64{first, second, third} {
		for k, v := range rows {
			g := got[k]
			got[k] = [2]uint64{g[0] + v[0], g[1] + v[1]}
		}
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("%+v: %v, want %v", k, got[k], w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("rows %v, want %v", got, want)
	}
	if len(third) != 0 {
		t.Errorf("a take with no bytes since the last returned %v", third)
	}

	after := scrape(t, p)
	to, from := pathSum(got, p)
	if float64(to) != after["to_agent"]-before["to_agent"] || float64(from) != after["from_agent"]-before["from_agent"] {
		t.Errorf("usage %d/%d, path counter %v/%v", to, from, after["to_agent"]-before["to_agent"], after["from_agent"]-before["from_agent"])
	}
	if n := liveMeters(); n != 0 {
		t.Errorf("%d meters still held after their sessions ended and were taken", n)
	}
}

// TestCountConnMetersItsSession: a counted conn with a meter counts its
// bytes for the meter's payer, in the conn's directions.
func TestCountConnMetersItsSession(t *testing.T) {
	EnableUsage()
	p := PathConnect
	takeAll()
	m := OpenMeter(p, "owner-c", "agent-c")
	x, y := net.Pipe()
	c := CountConn(x, p, m, time.Now(), true, true) // the agent's side
	go func() {
		_, _ = y.Write([]byte("hello"))
		_, _ = io.ReadFull(y, make([]byte, 3))
		_ = y.Close()
	}()
	if _, err := io.ReadFull(c, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write([]byte("abc"))
	_ = c.Close()
	m.End()
	if got := takeAll()[usageKey{"owner-c", "agent-c", p}]; got != [2]uint64{3, 5} {
		t.Fatalf("usage %v, want to_agent 3, from_agent 5", got)
	}
}

// TestUsageMeterAllocs: a session's bytes and a take of a live session
// allocate nothing.
func TestUsageMeterAllocs(t *testing.T) {
	EnableUsage()
	m := OpenMeter(PathSNI, "owner-d", "agent-d")
	defer m.End()
	takeAll()
	if n := testing.AllocsPerRun(1000, func() { AddBytes(PathSNI, m, 10, 20) }); n != 0 {
		t.Errorf("meter bytes: %v allocations, want 0", n)
	}
	AddBytes(PathSNI, m, 1, 1)
	TakeUsage(func(Usage) {})
	if n := testing.AllocsPerRun(100, func() { AddBytes(PathSNI, m, 1, 1); TakeUsage(func(Usage) {}) }); n != 0 {
		t.Errorf("take: %v allocations, want 0", n)
	}
}

// TestTransportPathsHaveNoUsage: bytes on a transport path (counted again
// on a frontend path) are never usage.
func TestTransportPathsHaveNoUsage(t *testing.T) {
	EnableUsage()
	takeAll()
	AddTransferBytes(PathTCPGRPC, 5, 5)
	AddTransferBytes(PathReplica, 5, 5)
	if m := OpenMeter(PathTCPLink, "owner-e", "agent-e"); m != nil {
		t.Errorf("a transport path's session got a meter")
	}
	if rows := takeAll(); len(rows) != 0 {
		t.Fatalf("transport bytes as usage: %v", rows)
	}
}

// TestUsageLiveMeterReportsAtEveryTake: a joined session's bytes (Live,
// counted whole only at its end: RecordChainEnded) show at every take
// while it lasts, and its end adds only what the last take had not seen:
// over all takes, its total exactly once, and the transfer counter the
// same.
func TestUsageLiveMeterReportsAtEveryTake(t *testing.T) {
	EnableUsage()
	p := PathConnect
	takeAll()
	before := scrape(t, p)
	m := OpenMeter(p, "owner-l", "agent-l")
	var in, out int64
	m.Live(func() (int64, int64) { return in, out })
	key := usageKey{"owner-l", "agent-l", p}

	in, out = 100, 5
	if got := takeAll()[key]; got != [2]uint64{100, 5} {
		t.Fatalf("first take %v, want the bytes moved so far", got)
	}
	in, out = 150, 5
	if got := takeAll()[key]; got != [2]uint64{50, 0} {
		t.Fatalf("second take %v, want only the new bytes", got)
	}
	in, out = 170, 9
	RecordChainEnded("connect", m, 170, 9)
	m.End()
	in, out = 1000, 1000 // a reading after the end counts nothing
	if got := takeAll()[key]; got != [2]uint64{20, 4} {
		t.Fatalf("take after the end %v, want the rest", got)
	}
	if got := takeAll()[key]; got != [2]uint64{} || liveMeters() != 0 {
		t.Fatalf("after: %v, %d meters live", got, liveMeters())
	}
	after := scrape(t, p)
	if to, from := after["to_agent"]-before["to_agent"], after["from_agent"]-before["from_agent"]; to != 170 || from != 9 {
		t.Errorf("transfer counter %v/%v, want 170/9", to, from)
	}
}
