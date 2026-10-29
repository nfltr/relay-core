// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// --- Gap 3: Broker publish latency ---

var (
	brokerPublishDurationHis = auto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: brokerNamespace,
			Name:      "publish_duration_milliseconds",
			Help:      "Latency of broker publish operations in milliseconds.",
			Buckets:   timingBuckets,
		},
	)
)

// RegisterBrokerPublishDuration records broker publish latency.
func RegisterBrokerPublishDuration(durationMs float64) {
	brokerPublishDurationHis.Observe(durationMs)
}

// --- Gap 4: Transaction registry size ---

var (
	txnRegistrySize = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "txn_registry_size",
			Help:      "Current number of entries in the transaction registry.",
		},
	)
)

// SetTxnRegistrySize sets the current transaction registry size.
func SetTxnRegistrySize(n int) {
	txnRegistrySize.Set(float64(n))
}

// --- Gap 5: Relay contention (active remote relays) ---

var (
	activeRemoteRelays = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "active_remote_relays",
			Help:      "Number of currently active remote (pod-to-pod) RelayTunnel relays on this pod.",
		},
	)
)

// IncrActiveRemoteRelays increments the active remote relay gauge.
func IncrActiveRemoteRelays() {
	activeRemoteRelays.Inc()
}

// DecrActiveRemoteRelays decrements the active remote relay gauge.
func DecrActiveRemoteRelays() {
	activeRemoteRelays.Dec()
}
