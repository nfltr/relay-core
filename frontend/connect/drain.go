// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"
	"sync"
	"time"

	"github.com/nfltr/relay/proto/wire/dataplane"
)

// A replica that leaves (SIGTERM, a rolling restart, a deploy) drains its
// clients' data connections (/connect, data.go) as it drains its
// peers' links: GOAWAY at once, so a client opens its next sessions on a
// new connection (another replica, where there is one: this one's HTTP
// listener is closed), while the sessions already open run to the drain's
// deadline, where each still open is aborted link.CodeDrainDeadline, never
// cut by the process's exit. A client before generation 3 (nfltr v2.1.6
// and older) gets no GOAWAY: its Open fails on a going-away connection
// instead of opening elsewhere, so its connection is served as before.
//
// A relay without peers (one replica: the standalone VM) has nowhere to
// send its agents, and its clients' sessions end at their agents: it keeps
// its TCP agents until its clients' data connections drained (their
// sessions ended, or the deadline cut them), then lets them all go at once
// and stops (the composition's drain). With nothing open it stops at once.

// drainedGeneration is the first client generation (agentwire) that opens
// elsewhere on GOAWAY.
const drainedGeneration = 3

// Clients are the data connections of clients served here: the
// composition drains them as the relay leaves (Drain) and waits for that
// (Drained, WaitDrained).
type Clients struct {
	mu       sync.Mutex
	conns    map[*dataplane.Mux]struct{}
	deadline time.Time     // the drain's; zero until it starts
	drained  chan struct{} // closed once drain returned
}

// Drained is closed once the drain returned.
func (c *Clients) Drained() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.drained == nil {
		c.drained = make(chan struct{})
	}
	return c.drained
}

// WaitDrained waits until the drain returned or bound passed, and returns
// how long it waited.
func (c *Clients) WaitDrained(bound time.Duration) time.Duration {
	start := time.Now()
	select {
	case <-c.Drained():
	case <-time.After(bound):
	}
	return time.Since(start)
}

// add holds m until done; a connection that arrives once the drain
// started is drained at once.
// ponytail: a late arrival's drain is not waited for by drain (the HTTP
// listener closed as the drain started: only an upgrade racing it).
func (c *Clients) add(m *dataplane.Mux) (done func()) {
	c.mu.Lock()
	if c.conns == nil {
		c.conns = map[*dataplane.Mux]struct{}{}
	}
	c.conns[m] = struct{}{}
	deadline := c.deadline
	c.mu.Unlock()
	if !deadline.IsZero() {
		go drainMux(m, deadline)
	}
	return func() {
		c.mu.Lock()
		delete(c.conns, m)
		c.mu.Unlock()
	}
}

// Drain drains every connection until deadline and returns once each
// closed: its sessions ended, or the deadline cut them.
func (c *Clients) Drain(deadline time.Time) {
	c.mu.Lock()
	c.deadline = deadline
	ms := make([]*dataplane.Mux, 0, len(c.conns))
	for m := range c.conns {
		ms = append(ms, m)
	}
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, m := range ms {
		wg.Go(func() { drainMux(m, deadline) })
	}
	wg.Wait()
	ch := c.Drained()
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-ch:
	default:
		close(c.drained)
	}
}

func drainMux(m *dataplane.Mux, deadline time.Time) {
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_ = m.Drain(ctx)
}
