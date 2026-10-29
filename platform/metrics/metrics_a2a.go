// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// --- Agent-to-agent (A2A) metrics ---

var (
	a2aRequestsTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "a2a_requests_total",
			Help:      "Total agent-to-agent requests by route type and outcome.",
		},
		[]string{"route_type", "status"},
	)

	a2aLatencyHis = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "a2a_latency_milliseconds",
			Help:      "End-to-end latency of agent-to-agent transactions in milliseconds.",
			Buckets:   timingBuckets,
		},
		[]string{"route_type"},
	)

	a2aErrorsTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "a2a_errors_total",
			Help:      "Total agent-to-agent errors by type.",
		},
		[]string{"error_type"},
	)

	a2aActiveSessions = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "a2a_active_sessions",
			Help:      "Number of currently active agent-to-agent routing sessions.",
		},
	)
)

// RegisterA2ARequest records a completed agent-to-agent request.
// routeType is "local" or "remote"; status is "ok", "error", "deadline_exceeded", "cancelled".
func RegisterA2ARequest(routeType, status string) {
	a2aRequestsTotal.WithLabelValues(routeType, status).Inc()
}

// RegisterA2ALatency records the end-to-end latency of an A2A transaction.
func RegisterA2ALatency(routeType string, durationMs float64) {
	a2aLatencyHis.WithLabelValues(routeType).Observe(durationMs)
}

// RegisterA2AError records an A2A error.
// errorType is "deadline_exceeded", "cancelled", "not_found", "routing_error", "remote_routing_error".
func RegisterA2AError(errorType string) {
	a2aErrorsTotal.WithLabelValues(errorType).Inc()
}

// IncrA2AActiveSessions increments the active A2A session gauge.
func IncrA2AActiveSessions() {
	a2aActiveSessions.Inc()
}

// DecrA2AActiveSessions decrements the active A2A session gauge.
func DecrA2AActiveSessions() {
	a2aActiveSessions.Dec()
}
