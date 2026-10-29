// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"net"
	"time"

	"golang.org/x/sync/errgroup"
)

// The core's wiring and drain, as every composition runs them
// (package compose): the TCP dispatcher beside the
// dispatcher, and the gRPC port's stop.
//
// Draining a replica (SIGTERM, a rolling restart): the readiness probe fails
// (the internal server stops) and every agent that moves (agent_move.go) is
// asked to move at once: it connects to another replica first and its
// record, so peers' routing, follows its new connection. An agent that does
// not move is let go at a random moment of the first half of the shutdown
// window (drainJitter) and redials, landing on another replica, spread out
// rather than all at once; its inventory record goes as it leaves
// (compare-and-delete, a record its new replica wrote is kept). Until no
// agent is routed here any more (WaitAgentsMoved) the replica link takes
// peers' sessions and forwards them to the agents here: no session is
// refused because its agent's replica leaves. Then the replica link and the
// gRPC port drain: sessions already open, on the replica link and on the
// agents' old links, run to the drain's deadline (Core.DrainBy), where
// each still open is aborted link.CodeDrainDeadline.
//
// A relay without peers (one replica) has nowhere to send its agents, and
// its clients' sessions end at their agents: it keeps its TCP agents until
// its clients' data connections drained (clientsDrained: the client-sessions
// frontend's), then lets them all go at once and stops.

// WireTCP makes the core's TCP dispatcher (agents' tcp services, ssh, the
// connect sidecar) and joins it to the dispatcher: heartbeats, the drain
// (until ctx ends with peers, until clientsDrained without), the agent
// link's deadline (DrainBy), renewals. The dispatcher, its replica link
// (PodDialer) and DrainBy are set before.
func (c *Core) WireTCP(ctx context.Context, logger *slog.Logger, grace time.Duration, clientsDrained <-chan struct{}) {
	d := c.Dispatcher
	t := NewTCPDispatcher(logger.With("subsystem", "tcp-tunnel"), d.PodDialer, c.StreamerIPInventory)
	if lpd, ok := d.PodDialer.(TCPLinkOpener); ok {
		t.Links = lpd
	}
	if d.Config.HeartbeatEnabled {
		t.ConfigureHeartbeats(d.Config.HeartbeatSendDuration, d.NotifyAgentHeartbeat)
	}
	if d.PodDialer != nil {
		t.ConfigureDrain(ctx.Done(), grace)
	} else {
		t.ConfigureDrain(clientsDrained, 0)
	}
	t.DrainBy = c.DrainBy // its agent link drains until then (agent_link.go)
	d.ConfigureTCPRenew(t)
	d.TCPDispatcher = t
	c.TCPDispatcher = t
}

// RoutedAgents is how many agents this replica still routes to: HTTP
// sessions not moved (a move takes its session out of routing once the
// agent's record names its new connection, agent_move.go) and the TCP
// streams of agents that do not move. The TCP stream of an agent that
// moves stays until the agent closes it with its old connection (its open
// sessions run on it): it moves with the agent's HTTP session.
func (c *Core) RoutedAgents() int {
	n := 0
	if d := c.Dispatcher; d != nil {
		n += len(d.GetAllConnectedAgents())
	}
	if t := c.TCPDispatcher; t != nil {
		n += t.UnmovedStreams()
	}
	return n
}

// WaitAgentsMoved waits until this replica routes to no agent or deadline
// passes.
func (c *Core) WaitAgentsMoved(deadline time.Time) {
	for time.Now().Before(deadline) && c.RoutedAgents() > 0 {
		time.Sleep(100 * time.Millisecond) // ponytail: shutdown-only poll
	}
}

// GRPCStopper is the agents' gRPC server as StopGRPC stops it.
type GRPCStopper interface {
	GracefulStop()
	Stop()
}

// StopGRPC stops the gRPC port once the drain lets it: with peers once
// this replica's agents moved, forced a second past the drain's deadline;
// without, once its clients drained (waitClients, bounded by the window),
// forced at the window's end. The agents' old links, drained as the port
// closes, carry their open sessions to the drain's deadline; the forced
// stop (which ends the agents' streams, and their links with them) comes
// after it.
func (c *Core) StopGRPC(logger *slog.Logger, s GRPCStopper, grace time.Duration, waitClients func(bound time.Duration) time.Duration) {
	var force time.Duration
	if c.Dispatcher != nil && c.Dispatcher.PodDialer != nil {
		c.WaitAgentsMoved(c.DrainBy())
		force = time.Until(c.DrainBy()) + time.Second
	} else {
		force = grace - waitClients(grace+time.Second)
	}
	stopped := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		logger.Info("relay: grpc gracefully stopped")
	case <-time.After(max(force, time.Second)):
		logger.Warn("relay: grpc graceful stop timed out, forcing stop")
		s.Stop()
	}
}

// ServeAgents serves the agents' gRPC port (serve on l: the port's
// listener, its busy agents' TCP data connections already taken off it by
// TCPDispatcher.DataPlaneListener, which the QUIC agent link needs first)
// until ctx ends and the drain lets it stop (StopGRPC, s): the stale-agent
// reaper runs while ctx lives, and fanout (HTTP ingress's; nil: none) hands
// agents' answers to the requests waiting for them until the port stopped,
// not only until the drain starts: a drained replica's peers forward to
// its agents for seconds after SIGTERM.
func (c *Core) ServeAgents(ctx context.Context, logger *slog.Logger, s GRPCStopper, serve func(net.Listener) error, l net.Listener, fanout func(context.Context) error, grace time.Duration, waitClients func(bound time.Duration) time.Duration) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, gctx := errgroup.WithContext(ctx)
	fanCtx, stopFanout := context.WithCancel(context.WithoutCancel(ctx))
	defer stopFanout()
	if fanout != nil {
		eg.Go(func() error {
			if err := fanout(fanCtx); fanCtx.Err() == nil {
				return err
			}
			return nil
		})
	}
	eg.Go(func() error { c.Dispatcher.RunStaleAgentReaper(gctx); return nil })
	eg.Go(func() error {
		<-gctx.Done()
		c.StopGRPC(logger, s, grace, waitClients)
		stopFanout()
		return nil
	})
	// The port is served on the caller's goroutine; when it ends (stopped,
	// or failed) the stop waiter and the reaper end too.
	logger.Info("serving gRPC", "address", l.Addr().String())
	err := serve(l)
	cancel()
	if werr := eg.Wait(); err == nil {
		err = werr
	}
	return err
}
