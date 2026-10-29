// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/internal/platform/tcprig"
	"github.com/nfltr/relay/client/agent"
	"github.com/nfltr/relay/proto/pb"
)

// The TCP-tunnel rig is core/session/sessiontest/rig; these bind it to the
// real TCPDispatcher.

type (
	tunnelHarness = tcprig.Harness[*TCPDispatcher]
	harnessRelay  = tcprig.HarnessRelay
	harnessAgent  = tcprig.HarnessAgent
)

const (
	harnessAgentID     = tcprig.AgentID
	harnessAgentHeader = tcprig.AgentHeader

	relayCurrent     = tcprig.RelayCurrent
	relayOneStream   = tcprig.RelayOneStream
	relayStreamsOnly = tcprig.RelayStreamsOnly
	relayNoDataPlane = tcprig.RelayNoDataPlane
	relayLinkV1      = tcprig.RelayLinkV1

	agentCurrent   = tcprig.AgentCurrent
	agentDataConns = tcprig.AgentDataConns
	agentAuxOnly   = tcprig.AgentAuxOnly
	agentOneConn   = tcprig.AgentOneConn
	agentDataFails = tcprig.AgentDataFails
	agentQUIC      = tcprig.AgentQUIC
)

// harnessAuxIdle, when set, is the harness agent link's idle time (the
// relay's auxiliary idle time).
var harnessAuxIdle time.Duration

var rigOptions = tcprig.Options[*TCPDispatcher]{
	New: func(logger *slog.Logger, auxIdle time.Duration) *TCPDispatcher {
		d := NewTCPDispatcher(logger, nil, nil)
		if auxIdle > 0 {
			d.AuxIdle = auxIdle
		}
		return d
	},
	BindHeader: TCPBindHeader,
	HandleConn: func(ctx context.Context, d *TCPDispatcher, c net.Conn, agentID, target string) {
		d.HandleConn(ctx, c, TCPEndpoint{AgentID: agentID, TargetAddr: target})
	},
	RouteConnection: func(ctx context.Context, d *TCPDispatcher, c net.Conn, agentID string) {
		d.RouteConnection(ctx, c, agentID, "", "")
	},
	Streams: func(d *TCPDispatcher, agentID string) (total, data int) {
		d.Mu.RLock()
		defer d.Mu.RUnlock()
		for _, as := range d.All[agentID] {
			total++
			if as.DC != nil {
				data++
			}
		}
		return total, data
	},
	AnyStream: func(d *TCPDispatcher) bool {
		d.Mu.RLock()
		defer d.Mu.RUnlock()
		return len(d.All) > 0
	},
}

func startTunnelHarness(tb testing.TB, target string, e2ee *agent.E2EESetup, backend string) *tunnelHarness {
	tb.Helper()
	return tcprig.StartTunnelHarness(tb, rigOptions, target, e2ee, backend)
}

func startTunnelHarnessRTT(tb testing.TB, target string, e2ee *agent.E2EESetup, backend string, rtt time.Duration) *tunnelHarness {
	tb.Helper()
	return tcprig.StartTunnelHarnessRTT(tb, rigOptions, target, e2ee, backend, rtt)
}

func startTunnelHarnessWith(tb testing.TB, target string, e2ee *agent.E2EESetup, backend string, rtt time.Duration, old harnessRelay, ag harnessAgent) *tunnelHarness {
	tb.Helper()
	return tcprig.StartTunnelHarnessWith(tb, rigOptions, target, e2ee, backend, rtt, old, ag, harnessAuxIdle)
}

func startHarness(tb testing.TB, rtt time.Duration, old harnessRelay, ag harnessAgent,
	run func(context.Context, pb.DispatcherClient, agent.RelayDialer, *slog.Logger),
	route func(context.Context, *TCPDispatcher, net.Conn)) *tunnelHarness {
	tb.Helper()
	return tcprig.StartHarness(tb, rigOptions, rtt, old, ag, harnessAuxIdle, run, route)
}

var (
	upload           = tcprig.Upload[*TCPDispatcher]
	download         = tcprig.Download[*TCPDispatcher]
	echo             = tcprig.Echo[*TCPDispatcher]
	runConcurrent    = tcprig.RunConcurrent
	startHeapSampler = tcprig.StartHeapSampler
	waitGoroutines   = tcprig.WaitGoroutines
	startTarget      = tcprig.StartTarget
	serveTargets     = tcprig.ServeTargets
	targetHeader     = tcprig.TargetHeader
	waitUntil        = tcprig.WaitUntil
)
