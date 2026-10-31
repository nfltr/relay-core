// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

// countAgents calls each with every connected agent's serves value
// (connected_agents, nfltr_relay_connected_agents_by_serves): an agent
// once, whether it holds its HTTP stream, its TCP streams or both; one
// with TCP streams only declared no services.
func (d *Dispatcher) countAgents(each func(serves string)) {
	seen := map[string]bool{}
	d.Agents.Range(func(id string, s *HTTPStream) bool {
		seen[id] = true
		each(servesOf(s.Services))
		return true
	})
	t := d.tcpAgents.Load()
	if t == nil {
		return
	}
	t.Mu.RLock()
	defer t.Mu.RUnlock()
	for id := range t.Streams {
		if !seen[id] {
			each(metrics.ServesNone)
		}
	}
}

// servesOf is the serves value of an agent's declared services.
func servesOf(decls []agentservices.Decl) string {
	var http, tcp bool
	for _, d := range decls {
		http = http || d.Kind == agentservices.HTTP
		tcp = tcp || d.Kind == agentservices.TCP
	}
	switch {
	case http && tcp:
		return metrics.ServesHTTPTCP
	case http:
		return metrics.ServesHTTP
	case tcp:
		return metrics.ServesTCP
	}
	return metrics.ServesNone
}
