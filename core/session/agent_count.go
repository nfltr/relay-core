// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"slices"
	"strings"

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

// servesOf is the serves value of an agent's declared services: the kinds
// present, http, tcp and udp in that order, joined by "_".
func servesOf(decls []agentservices.Decl) string {
	var kinds []string
	for _, k := range []agentservices.Kind{agentservices.HTTP, agentservices.TCP, agentservices.UDP} {
		if slices.ContainsFunc(decls, func(d agentservices.Decl) bool { return d.Kind == k }) {
			kinds = append(kinds, string(k))
		}
	}
	if len(kinds) == 0 {
		return metrics.ServesNone
	}
	return strings.Join(kinds, "_")
}
