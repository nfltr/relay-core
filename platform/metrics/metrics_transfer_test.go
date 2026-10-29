// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestTransferPathsClosed: the path label is the closed list the
// observability plan names (nfltr's data-path guard reads it too).
func TestTransferPathsClosed(t *testing.T) {
	want := []string{"http_link", "http_grpc", "sni", "connect", "ssh", "tcp_link", "tcp_quic", "tcp_dataconn",
		"tcp_grpc", "a2a", "replica", "wg", "p2p_relay", "mailbox", "blob", "accounts"}
	if got := PathNames(); !slices.Equal(got, want) {
		t.Fatalf("PathNames() = %v, want %v", got, want)
	}
	for p := range numPaths {
		if p.String() != want[p] {
			t.Errorf("Path(%d) = %q, want %q", p, p, want[p])
		}
	}
}

// TestTransferHotPathAllocs: what a session or a byte runs allocates
// nothing once its path's series exist.
func TestTransferHotPathAllocs(t *testing.T) {
	p := PathTCPGRPC
	TransferEnd(p, TransferBegin(p)) // resolves the path's series
	for name, f := range map[string]func(){
		"begin/end":  func() { TransferEnd(p, TransferBegin(p)) },
		"opened ok":  func() { TransferOpened(p, "ok") },
		"bytes":      func() { AddTransferBytes(p, 10, 20) },
		"stall":      func() { AddTransferStall(p, time.Millisecond) },
		"open":       func() { ObserveTransferOpen(p, time.Millisecond) },
		"first byte": func() { ObserveTransferFirstByte(p, time.Millisecond) },
		"observer":   func() { TransferObserver(p, true).CreditWait(time.Millisecond) },
	} {
		if n := testing.AllocsPerRun(1000, f); n != 0 {
			t.Errorf("%s: %v allocations, want 0", name, n)
		}
	}
}

// TestTransferConnCountersNeverGoBack: a tracked connection's bytes and
// credit waits stay counted after it ends; its queued bytes do not.
func TestTransferConnCountersNeverGoBack(t *testing.T) {
	p := PathTCPDataConn
	before := scrape(t, p)
	st := link.ConnStats{BytesIn: 100, BytesOut: 200, CreditWaits: 3, CreditWait: time.Second, QueuedBytes: 50}
	done := TrackTransferConn(p, func() link.ConnStats { return st })
	live := scrape(t, p)
	st.BytesIn, st.BytesOut = 150, 250
	done()
	done() // once
	ended := scrape(t, p)
	for k, want := range map[string]float64{"to_agent": 200, "from_agent": 100, "stalls": 3, "stall_seconds": 1, "queued": 50} {
		if got := live[k] - before[k]; got != want {
			t.Errorf("live %s = %v, want %v", k, got, want)
		}
	}
	for k, want := range map[string]float64{"to_agent": 250, "from_agent": 150, "stalls": 3, "stall_seconds": 1, "queued": 0} {
		if got := ended[k] - before[k]; got != want {
			t.Errorf("ended %s = %v, want %v", k, got, want)
		}
	}
}

// TestTransferLabelsBounded: results, reasons and abort codes outside
// their lists count as "other".
func TestTransferLabelsBounded(t *testing.T) {
	TransferOpened(PathConnect, "ERROR_REASON_AGENT_OFFLINE")
	TransferOpened(PathConnect, "anything a caller passes")
	TransferError(PathConnect, "something new")
	TransferError(PathConnect, TransferStalled)
	TransferAbort(PathConnect, link.CodeDraining)
	TransferAbort(PathConnect, link.CodeApp+7)
	TransferAbort(PathConnect, 999)
	for _, c := range []struct {
		vec    *prometheus.CounterVec
		labels []string
	}{
		{transferOpens, []string{"connect", "ERROR_REASON_AGENT_OFFLINE"}},
		{transferOpens, []string{"connect", "other"}},
		{transferErrors, []string{"connect", "other", "none"}},
		{transferErrors, []string{"connect", "stalled", "none"}},
		{transferErrors, []string{"connect", "abort", "draining"}},
		{transferErrors, []string{"connect", "abort", "app"}},
		{transferErrors, []string{"connect", "abort", "other"}},
	} {
		if testutil.ToFloat64(c.vec.WithLabelValues(c.labels...)) == 0 {
			t.Errorf("%v not counted", c.labels)
		}
	}
}

// scrape reads p's collector-computed transfer series.
func scrape(t *testing.T, p Path) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(transfers)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]float64{}
	for _, f := range fams {
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["path"] != p.String() {
				continue
			}
			v := m.GetCounter().GetValue() + m.GetGauge().GetValue()
			switch f.GetName() {
			case "nfltr_relay_transfer_bytes_total":
				out[labels["direction"]] = v
			case "nfltr_relay_transfer_stalls_total":
				out["stalls"] = v
			case "nfltr_relay_transfer_stall_seconds_total":
				out["stall_seconds"] = v
			case "nfltr_relay_transfer_queued_bytes":
				out["queued"] = v
			case "nfltr_relay_transfer_active":
				out["active"] = v
			}
		}
	}
	return out
}

// TestReplacedFamiliesStayRetired: the per-frontend families the transfer
// families replaced (nfltr's observability plan, phase OBS7) are not
// registered again; their sessions count on path sni, connect, replica and
// the tcp_* paths.
func TestReplacedFamiliesStayRetired(t *testing.T) {
	retired := []string{"_tcp_tunnel_", "_sni_router_", "_ssh_proxy_", "rpc_server_proxy_request_duration_milliseconds"}
	ch := make(chan *prometheus.Desc, 1024)
	all.Describe(ch)
	close(ch)
	for d := range ch {
		for _, name := range retired {
			if strings.Contains(d.String(), name) {
				t.Errorf("%s is registered again: %s", name, d)
			}
		}
	}
}
