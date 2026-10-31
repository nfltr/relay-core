// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestConnectedAgentsSumsSources: connected_agents and
// nfltr_relay_connected_agents_by_serves sum every live source's agents,
// and a source that reports itself gone is dropped.
func TestConnectedAgentsSumsSources(t *testing.T) {
	c := &agentCollector{total: agents.total, byServes: agents.byServes}
	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(c)
	gone := false
	c.srcs = []AgentSource{
		func(each func(string)) bool { each(ServesHTTP); each(ServesNone); return true },
		func(each func(string)) bool {
			if gone {
				return false
			}
			each(ServesHTTPTCP)
			return true
		},
	}
	want := func(total int, byServes string) {
		t.Helper()
		exp := "# HELP rpc_server_dispatcher_connected_agents Agents connected: each agent once, whatever streams it holds (HTTP, TCP or both).\n" +
			"# TYPE rpc_server_dispatcher_connected_agents gauge\n" +
			"rpc_server_dispatcher_connected_agents " + strconv.Itoa(total) + "\n" +
			"# HELP nfltr_relay_connected_agents_by_serves Agents connected, by the kinds of services each declared at connect (http, tcp, http_tcp; none: it declared none).\n" +
			"# TYPE nfltr_relay_connected_agents_by_serves gauge\n" + byServes
		if err := testutil.GatherAndCompare(reg, strings.NewReader(exp)); err != nil {
			t.Fatal(err)
		}
	}
	want(3, `nfltr_relay_connected_agents_by_serves{serves="http"} 1
nfltr_relay_connected_agents_by_serves{serves="http_tcp"} 1
nfltr_relay_connected_agents_by_serves{serves="none"} 1
nfltr_relay_connected_agents_by_serves{serves="tcp"} 0
`)
	gone = true
	want(2, `nfltr_relay_connected_agents_by_serves{serves="http"} 1
nfltr_relay_connected_agents_by_serves{serves="http_tcp"} 0
nfltr_relay_connected_agents_by_serves{serves="none"} 1
nfltr_relay_connected_agents_by_serves{serves="tcp"} 0
`)
	if len(c.srcs) != 1 {
		t.Fatalf("%d sources after one went, want 1", len(c.srcs))
	}
}
