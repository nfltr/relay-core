// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"time"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/grpcfactory"
	pb "github.com/nfltr/relay/proto/pb"
	"golang.org/x/sync/errgroup"
)

// AgentPort is the agents' gRPC port as every composition serves it
// (Serve, and a larger relay composing its own): the dispatcher's gRPC
// server authenticated by Auth, the busy agents' TCP data connections
// taken off Listener, the agent link over QUIC beside it, and
// Core.ServeAgents until the drain lets the port stop.
type AgentPort struct {
	Core   *session.Core
	Logger *slog.Logger
	Auth   *identity.Authenticator
	// PeerCertificates lets the gRPC server see the client's certificate
	// (an mTLS identity provider).
	PeerCertificates bool
	Listener         net.Listener
	// QUIC, with QUICTLS, serves the agent link over QUIC
	// (agent_link.quic.listen); nil: none.
	QUIC    net.PacketConn
	QUICTLS *tls.Config
	// Fanout is HTTP ingress's (nil: none); WaitClients the client
	// sessions' drain (Core.StopGRPC).
	Fanout      func(context.Context) error
	Grace       time.Duration
	WaitClients func(bound time.Duration) time.Duration
}

// Serve serves the port until ctx ends and the drain lets it stop. The
// data plane is taken off the listener first: the QUIC agent link needs
// it (TCPDispatcher.Link), so it comes before the QUIC link and before
// the gRPC server accepts (TestAgentLinkSurvivesReconnect).
func (p AgentPort) Serve(ctx context.Context) error {
	opts := []grpcfactory.GRPCServerOption{grpcfactory.WithAuthenticator(p.Auth)}
	if p.PeerCertificates {
		opts = append(opts, grpcfactory.WithPeerCertificates())
	}
	s := grpcfactory.NewGRPCServer(p.Logger, opts...)
	pb.RegisterDispatcherServer(s, p.Core.Dispatcher)
	l, c := p.Listener, p.Core
	var quic func(context.Context) error
	if c.TCPDispatcher != nil && l != nil {
		l = c.TCPDispatcher.DataPlaneListener(l)
		if p.QUIC != nil {
			var err error
			if quic, err = c.TCPDispatcher.AgentLinkQUIC(p.QUIC, p.QUICTLS); err != nil {
				return err
			}
			p.Logger.Info("agent link: QUIC", "addr", p.QUIC.LocalAddr())
		}
	}
	serve := func(ctx context.Context) error {
		return c.ServeAgents(ctx, p.Logger, s, s.Serve, l, p.Fanout, p.Grace, p.WaitClients)
	}
	if quic == nil {
		return serve(ctx)
	}
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return quic(ctx) })
	eg.Go(func() error { return serve(ctx) })
	return eg.Wait()
}
