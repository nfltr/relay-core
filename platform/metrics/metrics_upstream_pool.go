// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// --- Gap 1: AgentTunnel observability metrics ---

var (
	upstreamConnectionsTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "upstream_connections_total",
			Help:      "Total AgentTunnel connections established (gRPC and HTTP agents).",
		},
		[]string{"mode"},
	)

	upstreamConnectionDurationHis = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "upstream_connection_duration_seconds",
			Help:      "Duration of agent AgentTunnel connections in seconds.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 20), // 1s to ~145h
		},
		[]string{"disconnect_reason"},
	)

	upstreamMessagesProcessedTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "upstream_messages_processed_total",
			Help:      "Total messages processed from agents on the AgentTunnel path.",
		},
		[]string{"message_type"},
	)

	upstreamHeartbeatFailuresTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "upstream_heartbeat_failures_total",
			Help:      "Total heartbeat-related disconnections (send failure or recv timeout).",
		},
		[]string{"failure_type"},
	)
)

// RegisterUpstreamConnection increments the AgentTunnel connection counter.
// mode is "grpc" or "http".
func RegisterUpstreamConnection(mode string) {
	upstreamConnectionsTotal.WithLabelValues(mode).Inc()
}

// RegisterUpstreamConnectionDuration records how long an agent stayed connected.
// disconnectReason is "heartbeat_timeout", "heartbeat_send_fail", "server_restart", "clean", "error".
func RegisterUpstreamConnectionDuration(disconnectReason string, durationSec float64) {
	upstreamConnectionDurationHis.WithLabelValues(disconnectReason).Observe(durationSec)
}

// RegisterUpstreamMessageProcessed increments the message counter.
// messageType is "heartbeat", "data", "a2a".
func RegisterUpstreamMessageProcessed(messageType string) {
	upstreamMessagesProcessedTotal.WithLabelValues(messageType).Inc()
}

// RegisterUpstreamHeartbeatFailure records a heartbeat failure.
// failureType is "send_failure" or "recv_timeout".
func RegisterUpstreamHeartbeatFailure(failureType string) {
	upstreamHeartbeatFailuresTotal.WithLabelValues(failureType).Inc()
}
