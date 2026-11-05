// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import "context"

// AddHTTPSession registers agentID's HTTP session s here: requests route
// to it from now on, and requests waiting for it (RouteArriving) wake.
func (d *Dispatcher) AddHTTPSession(agentID string, s *HTTPStream) {
	d.Agents.Store(agentID, s)
	d.wakeArrivals()
}

// nextArrival is closed at the next HTTP session's registration here.
func (d *Dispatcher) nextArrival() <-chan struct{} {
	d.arrivalMu.Lock()
	defer d.arrivalMu.Unlock()
	if d.arrival == nil {
		d.arrival = make(chan struct{})
	}
	return d.arrival
}

// RouteArriving is RouteHere for an agent whose record names this replica
// though no session of it routes here: its session claims the record
// before it registers (dispatcher_subscribe.go), so the request waits up
// to agentStreamWait for that registration, woken by it, as the TCP path's
// streamFor does. ok false: the agent did not register here in time (a
// stale record): it is offline, never a forward to this replica itself.
// spec/Move.tla ArriveB, OwnerServes; spec/README.md finding 45.
func (d *Dispatcher) RouteArriving(ctx context.Context, agentID string) (release func(), ok bool) {
	ctx, cancel := context.WithTimeout(ctx, agentStreamWait)
	defer cancel()
	for {
		arrival := d.nextArrival()
		if release, ok := d.RouteHere(agentID); ok {
			return release, true
		}
		select {
		case <-arrival:
		case <-ctx.Done():
			return nil, false
		}
	}
}

// upstreamArriving is RouteArriving for an agent's gRPC stream (the
// agent-to-agent paths: RelayTunnel, routeAgentToAgent): self reports that
// agentID's streamer record names this replica though its stream is not in
// Upstreams; s is that stream once it registers within agentStreamWait
// (its session claims the record before it stores the stream:
// dispatcher_grpc.go), nil when it does not (a stale record: the agent is
// offline, never a dial to this replica itself). Move.tla ArriveB,
// OwnerServes; spec/README.md finding 45.
func (d *Dispatcher) upstreamArriving(ctx context.Context, agentID string) (s Streamer, self bool) {
	if d.StreamerIPInventory == nil {
		return nil, false
	}
	ip, err := d.StreamerIPInventory.GetIP(ctx, agentID)
	if err != nil || ip == "" || ip != d.Config.LoopbackAddr {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, agentStreamWait)
	defer cancel()
	for {
		arrival := d.nextArrival()
		if s, ok := d.Upstreams.Load(agentID); ok {
			return s, true
		}
		select {
		case <-arrival:
		case <-ctx.Done():
			return nil, true
		}
	}
}

// wakeArrivals wakes the requests waiting for an agent to register here
// (RouteArriving, upstreamArriving).
func (d *Dispatcher) wakeArrivals() {
	d.arrivalMu.Lock()
	if d.arrival != nil {
		close(d.arrival)
		d.arrival = nil
	}
	d.arrivalMu.Unlock()
}
