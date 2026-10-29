// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// --- gRPC RelayTunnel metrics ---

var (
	grpcDownstreamRequestsTotal = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "grpc_downstream_requests_total",
			Help:      "Total gRPC RelayTunnel requests by route and outcome.",
		},
		[]string{"status", "route"},
	)

	grpcDownstreamDurationHis = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "grpc_downstream_duration_milliseconds",
			Help:      "End-to-end duration of gRPC RelayTunnel calls in milliseconds.",
			Buckets:   timingBuckets,
		},
		[]string{"route"},
	)

	grpcDownstreamMessagesSent = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "grpc_downstream_messages_sent_total",
			Help:      "Total messages sent to agents via gRPC RelayTunnel.",
		},
	)

	grpcDownstreamMessagesReceived = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "grpc_downstream_messages_received_total",
			Help:      "Total response messages received from agents via gRPC RelayTunnel.",
		},
	)
)

// RegisterGRPCDownstreamRequest records a completed gRPC RelayTunnel request.
// route is "local" or "remote"; status is "ok" or "error".
func RegisterGRPCDownstreamRequest(status, route string) {
	grpcDownstreamRequestsTotal.WithLabelValues(status, route).Inc()
}

// RegisterGRPCDownstreamDuration records the duration of a gRPC RelayTunnel call.
func RegisterGRPCDownstreamDuration(route string, durationMs float64) {
	grpcDownstreamDurationHis.WithLabelValues(route).Observe(durationMs)
}

// IncrGRPCDownstreamMessagesSent increments the counter for messages sent to agents.
func IncrGRPCDownstreamMessagesSent() {
	grpcDownstreamMessagesSent.Inc()
}

// IncrGRPCDownstreamMessagesReceived increments the counter for response messages received from agents.
func IncrGRPCDownstreamMessagesReceived() {
	grpcDownstreamMessagesReceived.Inc()
}
