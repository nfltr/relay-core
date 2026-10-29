// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Buckets for the histogram (1-2-5 sequence, 1 ms → 10 s)
var timingBuckets = []float64{
	1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000,
}

const (
	proxyNamespace      = "rpc_server_proxy"
	dispatcherNamespace = "rpc_server_dispatcher"
	brokerNamespace     = "rpc_server_broker"
)

var (
	connectedAgents = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "connected_agents",
			Help:      "Number of rpc agents connected.",
		},
	)

	requestsQueued = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: brokerNamespace,
			Name:      "requests_queued_total",
			Help:      "The total number of requests queued to the broker.",
		},
	)

	requestsDequeued = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: brokerNamespace,
			Name:      "requests_dequeued_total",
			Help:      "The total number of requests dequeued from the broker.",
		},
	)

	requestsReceived = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "requests_received_total",
			Help:      "The total number of requests received.",
		},
	)

	requestsSentToAgents = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "requests_sent_to_agents_total",
			Help:      "The total number of requests sent to rpc agents.",
		},
	)

	responsesReceivedFromAgents = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "responses_received_from_agents_total",
			Help:      "The total number of responses received from rpc agents.",
		},
	)

	requestsFailedDueToAgentNotConnected = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "requests_failed_due_to_agent_not_connected_total",
			Help:      "The total number of requests failed due to agent not connected.",
		},
	)

	requestsTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "requests_total",
			Help:      "The total number of requests handled.",
		},
		[]string{"method", "status", "api_type"},
	)

	timeoutRequests = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "timeout_requests_total",
			Help:      "The total number of timed out requests.",
		},
	)

	rpcServerAgentLatencyHis = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: proxyNamespace,
			Name:      "rpc_server_agent_latency_milliseconds",
			Help:      "Histogram of latency introduced by RPC Server and RPC Agent",
			Buckets:   timingBuckets,
		},
		[]string{"method", "status"},
	)

	rpcRequestPayloadSize = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: proxyNamespace,
			Name:      "rpc_request_payload_size",
			Help:      "Histogram of rpc request payload size",
			Buckets:   prometheus.ExponentialBuckets(512*1024, 2, 20),
		},
		[]string{"method"}, // Labels for the histogram.
	)

	rpcResponsePayloadSize = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: proxyNamespace,
			Name:      "rpc_response_payload_size",
			Help:      "Histogram of rpc response payload size",
			Buckets:   prometheus.ExponentialBuckets(512*1024, 2, 20),
		},
		[]string{"method", "status"}, // Labels for the histogram.
	)
)

// RegisterAgentConnect counts a connected agent (connected_agents up one).
func RegisterAgentConnect() {
	connectedAgents.Inc()
}

// RegisterAgentDisconnect counts a disconnected agent (connected_agents down one).
func RegisterAgentDisconnect() {
	connectedAgents.Dec()
}

// IncrQueuedRequests counts a request queued to the broker.
func IncrQueuedRequests() {
	requestsQueued.Inc()
}

// IncrDequeuedRequests counts a request dequeued from the broker.
func IncrDequeuedRequests() {
	requestsDequeued.Inc()
}

// RegisterRequestsReceived counts a request received.
func RegisterRequestsReceived() {
	requestsReceived.Inc()
}

// RegisterRequestsSentToAgents counts a request sent to an agent.
func RegisterRequestsSentToAgents() {
	requestsSentToAgents.Inc()
}

// RegisterResponsesReceivedFromAgents counts a response received from an agent.
func RegisterResponsesReceivedFromAgents() {
	responsesReceivedFromAgents.Inc()
}

// RegisterRequestsTotal counts a handled request by method, status and API type.
func RegisterRequestsTotal(method, status, apiType string) {
	requestsTotal.WithLabelValues(httpMethod(method), httpStatus(status), apiType).Inc()
}

// RegisterTimeoutRequests counts a request that timed out.
func RegisterTimeoutRequests() {
	timeoutRequests.Inc()
}

// RegisterLatencyIntroduced observes the latency the relay and agent added, in milliseconds.
func RegisterLatencyIntroduced(method, status string, duration float64) {
	rpcServerAgentLatencyHis.WithLabelValues(httpMethod(method), httpStatus(status)).Observe(duration)
}

// RegisterRequestsFailedDueToAgentNotConnected counts a request that failed because its agent was not connected.
func RegisterRequestsFailedDueToAgentNotConnected() {
	requestsFailedDueToAgentNotConnected.Inc()
}

// RegisterRPCRequestPayloadSize observes a request payload's size in bytes.
func RegisterRPCRequestPayloadSize(method string, size float64) {
	rpcRequestPayloadSize.WithLabelValues(httpMethod(method)).Observe(size)
}

// RegisterRPCResponsePayloadSize observes a response payload's size in bytes.
func RegisterRPCResponsePayloadSize(method, status string, size float64) {
	rpcResponsePayloadSize.WithLabelValues(httpMethod(method), httpStatus(status)).Observe(size)
}
