// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"fmt"
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// agentLinkSeries is the agent link's open sessions and bytes of
// transport, by metric name (bytes by "{direction}").
func agentLinkSeries(t *testing.T, transport string) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		name := mf.GetName()
		if name != "rpc_server_dispatcher_agent_link_open_sessions" && name != "rpc_server_dispatcher_agent_link_bytes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["transport"] != transport {
				continue
			}
			key := name
			if d := labels["direction"]; d != "" {
				key += "{" + d + "}"
			}
			got[key] += metricValue(m)
		}
	}
	return got
}

// On each transport the agent link counts its bytes and its open sessions,
// which return to zero when they end and when the connection dies under
// them.
func TestAgentLinkBytesAndOpenSessions(t *testing.T) {
	const (
		open  = "rpc_server_dispatcher_agent_link_open_sessions"
		bytes = "rpc_server_dispatcher_agent_link_bytes_total"
	)
	for _, tc := range linkTransportAgents {
		t.Run(tc.transport, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, tc.ag)
			base := agentLinkSeries(t, tc.transport)
			grew := func(name string) float64 { return agentLinkSeries(t, tc.transport)[name] - base[name] }
			show := func() string { return fmt.Sprint(agentLinkSeries(t, tc.transport)) }

			if err := echo(h, t, []byte("dial")); err != nil {
				t.Fatal(err)
			}
			waitLinkTransport(t, h, tc.transport)
			const n = 1 << 20
			if err := upload(h, t, n); err != nil {
				t.Fatal(err)
			}
			if err := download(h, t, n); err != nil {
				t.Fatal(err)
			}
			waitUntil(t, func() bool { return grew(bytes+"{sent}") >= n && grew(bytes+"{received}") >= n },
				func() string { return fmt.Sprintf("bytes after %d each way: %s", n, show()) })
			waitUntil(t, func() bool { return grew(open) == 0 },
				func() string { return "open sessions after they ended: " + show() })

			c := h.Dial(t)  // a session held open: the target never reads
			defer c.Close() //nolint:errcheck
			if _, err := c.Write(targetHeader('h', 0)); err != nil {
				t.Fatal(err)
			}
			waitUntil(t, func() bool { return grew(open) == 1 },
				func() string { return "open sessions while one is open: " + show() })
			var conns []io.Closer // the relay's side of the agent link dies
			h.D.Link.Mu.Lock()
			for _, m := range h.D.Link.Conns {
				for lc := range m {
					conns = append(conns, lc)
				}
			}
			h.D.Link.Mu.Unlock()
			for _, lc := range conns {
				_ = lc.Close()
			}
			waitUntil(t, func() bool { return grew(open) == 0 },
				func() string { return "open sessions after their connection died: " + show() })
		})
	}
}

func metricValue(m *dto.Metric) float64 {
	switch {
	case m.Counter != nil:
		return m.Counter.GetValue()
	case m.Gauge != nil:
		return m.Gauge.GetValue()
	case m.Histogram != nil:
		return float64(m.Histogram.GetSampleCount())
	}
	return 0
}
