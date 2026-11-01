// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"sync/atomic"
)

// Usage: the bytes each frontend path carried, by the account that pays
// for them, for a caller that records usage (EnableUsage, TakeUsage). A
// session's payer is fixed when it opens (OpenMeter); its bytes count on
// its meter alone, which the path's transfer counter sums in at scrape, so
// the usage TakeUsage returns over time sums to that counter exactly:
// bytes counted without a meter are usage of no payer (Account ""). Only
// frontend paths have usage: a transport path carries the same bytes
// again. A session's bytes cost one atomic add on its own meter, and
// nothing runs between takes (TestUsageMeterAllocs).

// Usage is a payer's bytes on a path since the last TakeUsage. Account
// and AgentID are "" for bytes no meter counted; Account alone is "" for a
// session whose agent had no known owner.
type Usage struct {
	Account, AgentID   string
	Path               Path
	ToAgent, FromAgent uint64
}

// Meter counts one session's bytes on its path for its payer.
type Meter struct {
	p                Path
	account, agentID string
	n                [2]atomic.Uint64 // to_agent, from_agent
	ended            atomic.Bool
	taken            [2]uint64 // usage.mu
}

// frontendPaths are the paths with usage.
var frontendPaths = [numPaths]bool{
	PathHTTPLink: true, PathHTTPGRPC: true, PathSNI: true, PathConnect: true, PathSSH: true,
	PathA2A: true, PathWG: true, PathP2PRelay: true, PathMailbox: true, PathBlob: true, PathAccounts: true,
}

var usage = &usageMeters{live: map[*Meter]struct{}{}, rows: map[usageRow][2]uint64{}}

type usageMeters struct {
	enabled atomic.Bool

	takeMu sync.Mutex // one take at a time: rows is its scratch
	mu     sync.Mutex
	live   map[*Meter]struct{} // ended ones until the take after their end
	gone   [numPaths][2]uint64 // the bytes of meters no longer live
	// unmetered is how much of transfers.bytesN (bytes no meter counted)
	// the last take returned.
	unmetered [numPaths][2]uint64
	rows      map[usageRow][2]uint64
}

type usageRow struct {
	account, agentID string
	p                Path
}

// EnableUsage makes OpenMeter meter sessions from now on. Its caller takes
// the usage (TakeUsage) from then on, regularly: meters of ended sessions
// are let go at the take after their end.
func EnableUsage() { usage.enabled.Store(true) }

// UsageEnabled reports whether EnableUsage was called: a caller skips
// finding a session's payer when not.
func UsageEnabled() bool { return usage.enabled.Load() }

// OpenMeter is the meter of a session on p that account pays for, agentID
// its agent; nil (count without one) before EnableUsage or on a transport
// path. End it when the session ends.
func OpenMeter(p Path, account, agentID string) *Meter {
	if !usage.enabled.Load() || !frontendPaths[p] {
		return nil
	}
	m := &Meter{p: p, account: account, agentID: agentID}
	usage.mu.Lock()
	usage.live[m] = struct{}{}
	usage.mu.Unlock()
	return m
}

// AddBytes counts bytes of a session on p, to its agent and from it, on
// m, its meter, or without one (AddTransferBytes) when m is nil.
func AddBytes(p Path, m *Meter, toAgent, fromAgent int) {
	if m == nil {
		AddTransferBytes(p, toAgent, fromAgent)
		return
	}
	if toAgent > 0 {
		m.n[0].Add(uint64(toAgent))
	}
	if fromAgent > 0 {
		m.n[1].Add(uint64(fromAgent))
	}
}

// End ends m's session (nil: none): the take after it returns its last
// bytes and lets it go. Bytes added after that take count nowhere.
func (m *Meter) End() {
	if m != nil {
		m.ended.Store(true)
	}
}

// TakeUsage calls f with the usage since the last take: one Usage per
// payer, agent and path with bytes.
func TakeUsage(f func(Usage)) {
	u := usage
	u.takeMu.Lock()
	defer u.takeMu.Unlock()
	clear(u.rows)
	u.mu.Lock()
	for m := range u.live {
		u.take(m)
		if m.ended.Load() && m.n[0].Load() == m.taken[0] && m.n[1].Load() == m.taken[1] {
			delete(u.live, m) // nothing added since its take
			u.gone[m.p][0] += m.taken[0]
			u.gone[m.p][1] += m.taken[1]
		}
	}
	for p := range numPaths {
		if !frontendPaths[p] {
			continue
		}
		b := &transfers.bytesN[p]
		to, from := b[0].Load(), b[1].Load()
		u.add(usageRow{p: p}, [2]uint64{to - u.unmetered[p][0], from - u.unmetered[p][1]})
		u.unmetered[p] = [2]uint64{to, from}
	}
	u.mu.Unlock()
	for r, n := range u.rows {
		f(Usage{Account: r.account, AgentID: r.agentID, Path: r.p, ToAgent: n[0], FromAgent: n[1]})
	}
}

// take adds m's bytes since its last take to rows (u.mu held).
func (u *usageMeters) take(m *Meter) {
	to, from := m.n[0].Load(), m.n[1].Load()
	u.add(usageRow{m.account, m.agentID, m.p}, [2]uint64{to - m.taken[0], from - m.taken[1]})
	m.taken = [2]uint64{to, from}
}

func (u *usageMeters) add(r usageRow, d [2]uint64) {
	if d == [2]uint64{} {
		return
	}
	v := u.rows[r]
	u.rows[r] = [2]uint64{v[0] + d[0], v[1] + d[1]}
}

// meteredBytes is the bytes meters counted on each path, for the transfer
// counters.
func meteredBytes() (out [numPaths][2]uint64) {
	u := usage
	u.mu.Lock()
	defer u.mu.Unlock()
	out = u.gone
	for m := range u.live {
		out[m.p][0] += m.n[0].Load()
		out[m.p][1] += m.n[1].Load()
	}
	return out
}

// liveMeters is how many meters usage holds (tests).
func liveMeters() int {
	usage.mu.Lock()
	defer usage.mu.Unlock()
	return len(usage.live)
}
