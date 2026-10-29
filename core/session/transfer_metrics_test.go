// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// transferSeries sums the nfltr_relay_transfer_* series of paths, by
// metric name (bytes by "{direction}", opens by "{result}").
func transferSeries(t *testing.T, paths ...string) map[string]float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, p := range paths {
		want[p] = true
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if !want[labels["path"]] {
				continue
			}
			key := mf.GetName()
			if d := labels["direction"] + labels["result"]; d != "" {
				key += "{" + d + "}"
			}
			got[key] += metricValue(m)
		}
	}
	return got
}

// Every agent transport counts its sessions (opens, active, lifetimes)
// and its bytes while they flow, on its own path.
func TestTransferFamiliesPerTransport(t *testing.T) {
	const (
		opens    = "nfltr_relay_transfer_opens_total{ok}"
		active   = "nfltr_relay_transfer_active"
		duration = "nfltr_relay_transfer_duration_seconds"
		to       = "nfltr_relay_transfer_bytes_total{to_agent}"
		from     = "nfltr_relay_transfer_bytes_total{from_agent}"
	)
	for _, tc := range []struct {
		name  string
		relay harnessRelay
		ag    harnessAgent
		paths []string // a session takes one of them
	}{
		{"tcp_link", relayCurrent, agentCurrent, []string{"tcp_link"}},
		{"tcp_quic", relayCurrent, agentQUIC, []string{"tcp_quic"}},
		{"tcp_dataconn", relayCurrent, agentDataConns, []string{"tcp_dataconn", "tcp_grpc"}},
		{"tcp_grpc", relayStreamsOnly, agentCurrent, []string{"tcp_grpc"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, tc.relay, tc.ag)
			base := transferSeries(t, tc.paths...)
			grew := func(name string) float64 { return transferSeries(t, tc.paths...)[name] - base[name] }
			show := func() string { return fmt.Sprint(transferSeries(t, tc.paths...)) }

			const n = 1 << 20
			if err := upload(h, t, n); err != nil {
				t.Fatal(err)
			}
			if err := download(h, t, n); err != nil {
				t.Fatal(err)
			}
			waitUntil(t, func() bool {
				return grew(to) >= n && grew(from) >= n && grew(opens) >= 2 && grew(duration) >= 2 && grew(active) == 0
			}, func() string { return fmt.Sprintf("after %d bytes each way: %s", n, show()) })
		})
	}
}
