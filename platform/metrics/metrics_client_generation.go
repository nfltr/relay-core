// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// The client generation (agentwire.GenerationKey) and the TCP data plane's
// link versions, so an operator sees which clients still connect before
// switching on security.min_client_generation.
var (
	clientsRefused = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "clients_refused_total",
		Help:      "Calls and tcp-connect requests refused at connect, by reason (generation: below security.min_client_generation).",
	}, []string{"reason"})
	oldGenerationClients = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "old_generation_clients_total",
		Help:      "Calls and tcp-connect requests of clients below the current generation (3: link chaining; 2: nfltr v2.0.0 to v2.1.6; 0: before v2.0.0) taken under security.min_client_generation, by transport (grpc, http).",
	}, []string{"transport"})
	dataConnections = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "data_connections_total",
		Help:      "TCP data-plane connections by the link wire version of their hello (1: refused, clients before v2.0.0) and kind (agent: the gRPC port; connect: tcp-connect's upgrade).",
	}, []string{"version", "kind"})
)

// RecordGenerationRefusal counts one call or request refused for its client
// generation. allow:wrapper
func RecordGenerationRefusal() { clientsRefused.WithLabelValues("generation").Inc() }

// RecordOldGenerationClient counts one call or request of a client below
// the current generation taken; transport is "grpc" or "http".
// allow:wrapper
func RecordOldGenerationClient(transport string) {
	oldGenerationClients.WithLabelValues(transport).Inc()
}

// agentHTTPRequests counts the requests the relay hands to an agent on its
// replica by the path they take there.
var agentHTTPRequests = auto.NewCounterVec(prometheus.CounterOpts{
	Namespace: dispatcherNamespace,
	Name:      "agent_http_requests_total",
	Help:      "HTTP requests handed to an agent on this replica, by path (link: a session on the agent link or a TCP stream; grpc: ConnectHTTP messages).",
}, []string{"path"})

// RecordAgentHTTPRequest counts one request to an agent on path ("link",
// "grpc"). allow:wrapper
func RecordAgentHTTPRequest(path string) { agentHTTPRequests.WithLabelValues(path).Inc() }

// RecordDataConnection counts one data-connection hello of link wire
// version (bounded: "1", "2", else "other") on kind ("agent", "connect").
func RecordDataConnection(version int, kind string) {
	v := "other"
	switch version {
	case 1:
		v = "1"
	case 2:
		v = "2"
	}
	dataConnections.WithLabelValues(v, kind).Inc()
}
