// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay/proto/pb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// An A2A call to a target not connected anywhere (single relay: no pod
// dialer, NotFound) is refused as the agent offline, as on the local
// path, never as "other".
func TestA2ARemoteNotFoundCountsAgentOffline(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	streamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 1), ctx: ctx}
	src := newTestGRPCStream("agent-a", streamer, ctx)
	msg := &pb.TunnelFrame{TransactionId: "txn-offline", Routing: &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-gone"}}
	const offline = "nfltr_relay_transfer_opens_total{ERROR_REASON_AGENT_OFFLINE}"
	const other = "nfltr_relay_transfer_opens_total{other}"
	before := transferSeries(t, "a2a")
	d.routeAgentToAgentRemote(ctx, src, nil, msg, "agent-gone", "agent-a", time.Now(), d.Logger, nil, status.Error(codes.NotFound, "no pod dialer"))
	after := transferSeries(t, "a2a")
	if after[offline]-before[offline] != 1 || after[other] != before[other] {
		t.Fatalf("offline %v -> %v, other %v -> %v; want offline +1, other unchanged", before[offline], after[offline], before[other], after[other])
	}
}

// An A2A call to a target connected to another replica this one could not
// reach (the replica link's no-route refusal) is answered as such,
// Unavailable with the refusal's message, never "target agent not
// connected", and is not counted as the agent offline.
func TestA2ARemoteUnreachableReplicaIsNotAgentOffline(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	streamer := &channelStreamer{sendCh: make(chan *pb.TunnelFrame, 1), ctx: ctx}
	src := newTestGRPCStream("agent-a", streamer, ctx)
	msg := &pb.TunnelFrame{TransactionId: "txn-far", Routing: &pb.RoutingInfo{AgentId: "agent-a", TargetAgentId: "agent-far"}}
	const why = "agent agent-far is connected to another replica of the relay, which could not be reached; try again"
	dialErr := fmt.Errorf("%w: %w", &link.Abort{Refused: true, Code: link.CodeNoRoute, Msg: why}, status.Error(codes.Unavailable, "replica link: dial timeout"))
	const offline = "nfltr_relay_transfer_opens_total{ERROR_REASON_AGENT_OFFLINE}"
	before := transferSeries(t, "a2a")
	d.routeAgentToAgentRemote(ctx, src, nil, msg, "agent-far", "agent-a", time.Now(), d.Logger, nil, dialErr)
	after := transferSeries(t, "a2a")
	if after[offline] != before[offline] {
		t.Errorf("offline %v -> %v; an unreachable replica is not the agent offline", before[offline], after[offline])
	}
	select {
	case got := <-streamer.sendCh:
		if codes.Code(got.GetStatusCode()) != codes.Unavailable || got.GetStatusDetail() != why {
			t.Fatalf("answer %v %q, want Unavailable %q", codes.Code(got.GetStatusCode()), got.GetStatusDetail(), why)
		}
	case <-ctx.Done():
		t.Fatal("no answer sent to the source agent")
	}
}
