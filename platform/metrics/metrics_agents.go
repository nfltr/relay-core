// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Serves values of nfltr_relay_connected_agents_by_serves: the kinds of
// the services an agent declared at connect.
const (
	ServesHTTP    = "http"
	ServesTCP     = "tcp"
	ServesHTTPTCP = "http_tcp"
	ServesNone    = "none" // declared none
)

var servesValues = [...]string{ServesHTTP, ServesTCP, ServesHTTPTCP, ServesNone}

// AgentSource is one relay's connected agents as a scrape counts them: it
// calls each with the agent's serves value (each agent once), and returns
// false once the relay is gone (the source is then dropped).
type AgentSource func(each func(serves string)) bool

// TrackAgents adds src to connected_agents and
// nfltr_relay_connected_agents_by_serves.
func TrackAgents(src AgentSource) {
	agents.mu.Lock()
	agents.srcs = append(agents.srcs, src)
	agents.mu.Unlock()
}

var agents = register(&agentCollector{
	total: prometheus.NewDesc(dispatcherNamespace+"_connected_agents",
		"Agents connected: each agent once, whatever streams it holds (HTTP, TCP or both).", nil, nil),
	byServes: prometheus.NewDesc("nfltr_relay_connected_agents_by_serves",
		"Agents connected, by the kinds of services each declared at connect (http, tcp, http_tcp; none: it declared none).", []string{"serves"}, nil),
})

type agentCollector struct {
	total, byServes *prometheus.Desc
	mu              sync.Mutex
	srcs            []AgentSource
}

func (c *agentCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.total
	ch <- c.byServes
}

func (c *agentCollector) Collect(ch chan<- prometheus.Metric) {
	n := map[string]int{}
	total := 0
	c.mu.Lock()
	live := c.srcs[:0]
	for _, src := range c.srcs {
		if src(func(serves string) { n[serves]++; total++ }) {
			live = append(live, src)
		}
	}
	clear(c.srcs[len(live):])
	c.srcs = live
	c.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(total))
	for _, s := range servesValues {
		ch <- prometheus.MustNewConstMetric(c.byServes, prometheus.GaugeValue, float64(n[s]), s)
	}
}
