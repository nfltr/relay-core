// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package publictcp

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/connlimit"
	"github.com/nfltr/relay-core/platform/metrics"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"github.com/spf13/viper"
)

// Leases is what the public ports need of the relay's lease store: the
// lease on a port, and each lease the journal ends on this relay.
type Leases interface {
	// PortGrant reads the lease on port: ErrNoGrant for none, any other
	// error when the store could not answer.
	PortGrant(port int) (Grant, error)
	// OnLeaseEnd registers cut, called with the share id of every lease
	// whose end (revoke, release, expiry, reclaim) this relay applies.
	OnLeaseEnd(cut func(shareID string))
}

// leaseHost is a host that leases public ports (the hosted relay).
type leaseHost interface{ PublicPorts() Leases }

// Frontend is public TCP ports as a binary links it.
type Frontend struct{}

// Name implements frontend.Frontend.
func (Frontend) Name() string { return "publictcp" }

// Settings implements frontend.Frontend: the public_tcp.* settings.
func (Frontend) Settings() []frontend.Setting {
	return []frontend.Setting{
		{Name: "public_tcp.ports", Default: "", Usage: "public TCP port range lo-hi leased to agents' plain tcp services declared public (empty: off); bound whole at start, below the ephemeral range"},
		{Name: "public_tcp.listen", Default: "", Usage: "address the public TCP ports listen on (empty: every address)"},
		{Name: "public_tcp.idle_timeout", Default: 15 * time.Minute, Usage: "a public TCP visit with no byte either way this long is ended (0: never)"},
		{Name: "public_tcp.source_max", Default: 8, Usage: "open public TCP connections per source address across the range (0: no limit)"},
		{Name: "public_tcp.source_rate", Default: 4, Usage: "new public TCP connections a second per source address (0: no limit)"},
		{Name: "public_tcp.source_burst", Default: 8, Usage: "burst of public_tcp.source_rate"},
	}
}

// Attach implements frontend.Frontend: with public_tcp.ports, a host that
// leases ports and tcp sessions, it binds the range and serves it.
func (Frontend) Attach(ctx context.Context, h frontend.Host) error {
	spec := viper.GetString("public_tcp.ports")
	if spec == "" {
		return nil
	}
	core := h.Core()
	lh, ok := h.(leaseHost)
	if !ok || core.TCPDispatcher == nil || core.Dispatcher == nil {
		return errors.New("public tcp: public_tcp.ports needs tcp sessions and a relay that leases ports")
	}
	if viper.GetString("tcp.e2ee_policy") == config.E2EERequire {
		return errors.New("public tcp: public ports carry plain TCP; tcp.e2ee_policy=require forbids them")
	}
	lo, hi, err := ParseRange(spec)
	if err != nil {
		return err
	}
	if elo, ehi, err := EphemeralRange(); err == nil {
		if err := CheckEphemeral(lo, hi, elo, ehi); err != nil {
			return err
		}
	}
	lns, err := Bind(viper.GetString("public_tcp.listen"), lo, hi)
	if err != nil {
		return err
	}
	leases := lh.PublicPorts()
	logger := h.Logger().With("subsystem", "public-tcp")
	srv := New(Config{
		Lookup: leases.PortGrant,
		Route:  route(core),
		Log:    logger,
		Source: connlimit.Config{
			MaxPerIP: viper.GetInt("public_tcp.source_max"),
			Rate:     float64(viper.GetInt("public_tcp.source_rate")),
			Burst:    viper.GetInt("public_tcp.source_burst"),
		},
		IdleTimeout: viper.GetDuration("public_tcp.idle_timeout"),
	})
	leases.OnLeaseEnd(func(id string) { srv.Cut(id) })
	logger.Info("public tcp ports listening: the relay carries these bytes in plain", "ports", spec, "addr", viper.GetString("public_tcp.listen"))
	h.Go(func() error {
		err := srv.Serve(ctx, lns)
		srv.Wait()
		return err
	})
	return nil
}

// route carries a visit to its agent's service: only a service the live
// agent declared public (its machine's own opt-in, beside the account's
// lease), within the account's visitor limits, metered to the owner.
func route(core *session.Core) func(ctx context.Context, conn net.Conn, g Grant) {
	d := core.TCPDispatcher
	return func(ctx context.Context, conn net.Conn, g Grant) {
		decls, live := core.Dispatcher.AgentServices(g.AgentID)
		if !live || !declaredPublic(decls, g.Service) {
			metrics.TransferOpened(metrics.PathPublicTCP, relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE.String())
			return
		}
		release, reason := d.AdmitVisitor(g.AgentID, sourceIP(conn))
		if reason != "" {
			metrics.IncrConnRefusals("public_tcp", reason)
			metrics.TransferOpened(metrics.PathPublicTCP, relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED.String())
			return
		}
		defer release()
		began := metrics.TransferBegin(metrics.PathPublicTCP)
		defer metrics.TransferEnd(metrics.PathPublicTCP, began)
		meter := d.OpenShareMeter(metrics.PathPublicTCP, g.AgentID, g.ShareID)
		defer meter.End()
		visitor := metrics.CountConn(conn, metrics.PathPublicTCP, meter, began, false, true)
		d.HandleConn(ctx, visitor, session.TCPEndpoint{AgentID: g.AgentID, TargetAddr: g.Service})
	}
}

// declaredPublic reports whether decls declare service a plain tcp
// service open to the public (agentservices ";public").
func declaredPublic(decls []agentservices.Decl, service string) bool {
	d, ok := agentservices.Find(decls, service)
	return ok && d.Public && d.Kind == agentservices.TCP
}
