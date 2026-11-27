// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/frontend/connect"
	"golang.org/x/sync/errgroup"
)

// Composition is a relay's run on the core: every binary that composes
// the core (Serve, or a larger relay of its own) fills it and calls Run.
// The composition owns
// the lifecycle (one group, the agent port, the internal and metrics
// ports, the client sessions' drain); the binary owns what it serves,
// through the hooks.
type Composition struct {
	Core   *session.Core
	Logger *slog.Logger
	// Internal serves InternalHandler (readiness, health) until the drain
	// starts: readiness fails at once. Nil: no internal port.
	Internal        net.Listener
	InternalHandler http.Handler
	// Metrics serves the metrics handler (Metrics) until the agent port
	// stopped, so the drain can be scraped; without an agent port until
	// the drain starts. Nil: no metrics port.
	Metrics net.Listener
	// Clients are the client sessions' data connections (frontend/connect),
	// drained to Core.DrainBy when the drain starts; nil: none.
	Clients *connect.Clients
	// Start runs before anything serves, with the run's context and its
	// Go: frontends attach (frontend.Host.Go is run), the replica link
	// starts. An error ends the run.
	Start func(ctx context.Context, run func(func() error)) error
	// Agents is the agents' port, read after Start (frontends may wrap
	// its listener); nil, or nil returned: no agent port.
	Agents func() *AgentPort
	// Serve are the binary's own servers and loops (the proxy port, the
	// replica directory's upkeep, ...), each until ctx ends.
	Serve []func(ctx context.Context) error
	// After runs once everything returned (a final flush).
	After func()
}

// Run runs c until ctx ends and every server drained, or one fails.
func (c *Composition) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)
	if c.Start != nil {
		if err := c.Start(ctx, eg.Go); err != nil {
			cancel()
			_ = eg.Wait()
			return err
		}
	}
	if c.Clients != nil {
		eg.Go(func() error {
			<-ctx.Done()
			c.Clients.Drain(c.Core.DrainBy())
			return nil
		})
	}
	var metrics *http.Server
	if c.Metrics != nil {
		metrics = &http.Server{Handler: Metrics(), ReadHeaderTimeout: 10 * time.Second}
		eg.Go(func() error { return serveHTTP(metrics, c.Metrics) })
	}
	var agents *AgentPort
	if c.Agents != nil {
		agents = c.Agents()
	}
	switch {
	case agents != nil:
		eg.Go(func() error {
			err := agents.Serve(ctx)
			if metrics != nil {
				_ = metrics.Close()
			}
			return err
		})
	case metrics != nil:
		eg.Go(func() error { <-ctx.Done(); return metrics.Close() })
	}
	if c.Internal != nil {
		internal := &http.Server{Handler: c.InternalHandler, ReadHeaderTimeout: 10 * time.Second}
		eg.Go(func() error { return serveHTTP(internal, c.Internal) })
		eg.Go(func() error { <-ctx.Done(); return internal.Close() })
	}
	for _, s := range c.Serve {
		eg.Go(func() error { return s(ctx) })
	}
	err := eg.Wait()
	if c.After != nil {
		c.After()
	}
	return err
}

// serveHTTP serves s on l until s is closed.
func serveHTTP(s *http.Server, l net.Listener) error {
	if err := s.Serve(l); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
