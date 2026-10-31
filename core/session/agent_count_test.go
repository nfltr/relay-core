// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"maps"
	"testing"

	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

// TestCountAgentsCountsEveryConnectedAgent: an agent counts once whether
// it holds its HTTP stream, TCP streams or both (TCP-only agents were
// missing from connected_agents), by the kinds of services it declared.
func TestCountAgentsCountsEveryConnectedAgent(t *testing.T) {
	d := &Dispatcher{Agents: &util.SyncMap[string, *HTTPStream]{}}
	d.Agents.Store("web", &HTTPStream{Services: []agentservices.Decl{{Name: "site", Kind: agentservices.HTTP}}})
	d.Agents.Store("both", &HTTPStream{Services: []agentservices.Decl{{Name: "site", Kind: agentservices.HTTP}, {Name: "db", Kind: agentservices.TCP}}})
	d.Agents.Store("old", &HTTPStream{})
	count := func() map[string]int {
		n := map[string]int{}
		d.countAgents(func(s string) { n[s]++ })
		return n
	}
	if got, want := count(), map[string]int{"http": 1, "http_tcp": 1, "none": 1}; !maps.Equal(got, want) {
		t.Fatalf("without a TCP dispatcher: %v, want %v", got, want)
	}
	tcp := &TCPDispatcher{Streams: map[string]*tcpAgentStream{"both": {}, "tcponly": {}}}
	d.tcpAgents.Store(tcp)
	if got, want := count(), map[string]int{"http": 1, "http_tcp": 1, "none": 2}; !maps.Equal(got, want) {
		t.Fatalf("with a TCP-only agent: %v, want %v", got, want)
	}
}
