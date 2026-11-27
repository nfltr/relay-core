// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

// TestAgentLinkFirstSessionMeasure measures a session's time to its first
// echoed byte (visitor connect, open, one byte each way), on loopback:
// "lazy" is the first session of an idle agent (the relay asks it to dial
// its link: the link retires between rounds), "up" a session on a link
// already up (what every session cost with the link dialled at start),
// "grpc" a session on the primary's frame stream (an agent without the
// agent link). AGENT_LINK_MEASURE=<rounds>; AGENT_LINK_MEASURE_RTT=<duration>
// puts that round trip between the agent and the relay's TCP port (QUIC is
// then not measured: its UDP path has no delay).
func TestAgentLinkFirstSessionMeasure(t *testing.T) {
	rounds, _ := strconv.Atoi(os.Getenv("AGENT_LINK_MEASURE"))
	if rounds <= 0 {
		t.Skip("measurement: AGENT_LINK_MEASURE=<rounds>")
	}
	rtt, _ := time.ParseDuration(os.Getenv("AGENT_LINK_MEASURE_RTT"))
	harnessAuxIdle = 50 * time.Millisecond
	t.Cleanup(func() { harnessAuxIdle = 0 })
	firstByte := func(h *tunnelHarness) time.Duration {
		start := time.Now()
		c := h.Dial(t)
		defer func() { _ = c.Close() }()
		_, _ = c.Write(append(targetHeader('e', 0), 'x'))
		if _, err := io.ReadFull(c, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
		return time.Since(start)
	}
	report := func(name string, ds []time.Duration) {
		slices.Sort(ds)
		t.Logf("%-10s n=%d p50=%s p95=%s max=%s", name, len(ds), ds[len(ds)/2], ds[len(ds)*95/100], ds[len(ds)-1])
	}
	for _, tc := range linkTransportAgents {
		if rtt > 0 && tc.ag == agentQUIC {
			continue
		}
		h := startTunnelHarnessWith(t, startTarget(t), nil, "", rtt, relayCurrent, tc.ag)
		var lazy, up []time.Duration
		for range rounds {
			waitUntil(t, func() bool { return linkConns(h) == 0 }, func() string { return "the link did not retire" })
			lazy = append(lazy, firstByte(h))
			c := h.Dial(t) // holds the link up for the next one
			_, _ = c.Write(append(targetHeader('e', 0), 'x'))
			_, _ = io.ReadFull(c, make([]byte, 1))
			up = append(up, firstByte(h))
			_ = c.Close()
		}
		report(tc.transport+"/lazy", lazy)
		report(tc.transport+"/up", up)
	}
	h := startTunnelHarnessWith(t, startTarget(t), nil, "", rtt, relayCurrent, agentAuxOnly)
	var grpc []time.Duration
	for range rounds {
		grpc = append(grpc, firstByte(h))
	}
	report("grpc", grpc)
}
