// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// --- Connection-break and reliability metrics ---

var (
	podToPodConnectionFailures = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "pod_to_pod_connection_failures_total",
			Help:      "Total pod-to-pod gRPC connection/stream failures.",
		},
		[]string{"target_pod"},
	)

	brokerMessageDecodeErrors = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: brokerNamespace,
			Name:      "message_decode_errors_total",
			Help:      "Total broker messages that failed to decode.",
		},
	)

	fanoutResponseDrops = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "fanout_response_drops_total",
			Help:      "Total fanout responses dropped due to context cancellation.",
		},
		[]string{"reason"},
	)

	brokerAckFailures = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: brokerNamespace,
			Name:      "ack_failures_total",
			Help:      "Total broker message acknowledgement failures.",
		},
	)

	downstreamTransactionTimeouts = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "downstream_transaction_timeouts_total",
			Help:      "Total downstream transactions that timed out waiting for agent response.",
		},
	)
)

// RegisterPodToPodConnectionFailure counts a failed connection to another replica, by its address.
func RegisterPodToPodConnectionFailure(targetPod string) {
	podToPodConnectionFailures.WithLabelValues(targetPod).Inc()
}

// RegisterBrokerMessageDecodeError counts a broker message that failed to decode.
func RegisterBrokerMessageDecodeError() {
	brokerMessageDecodeErrors.Inc()
}

// RegisterFanoutResponseDrop counts a fanout response dropped, by reason.
func RegisterFanoutResponseDrop(reason string) {
	fanoutResponseDrops.WithLabelValues(reason).Inc()
}

// RegisterBrokerAckFailure counts a failed broker acknowledgement.
func RegisterBrokerAckFailure() {
	brokerAckFailures.Inc()
}

// RegisterDownstreamTransactionTimeout counts a transaction that timed out waiting for its agent's answer.
func RegisterDownstreamTransactionTimeout() {
	downstreamTransactionTimeouts.Inc()
}
