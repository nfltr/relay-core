// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// Share Code metrics
// ---------------------------------------------------------------------------

var (
	shareCodesIssued = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "share_codes_issued_total",
			Help:      "Total share codes issued.",
		},
	)
	shareCodesValidated = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "share_codes_validated_total",
			Help:      "Total share code validation attempts.",
		},
		[]string{"result"}, // result: "valid" or "invalid"
	)
	shareCodesActive = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "share_codes_active",
			Help:      "Current number of active (non-expired) share codes.",
		},
	)
)

// IncrShareCodesIssued records a new share code issuance.
func IncrShareCodesIssued() {
	shareCodesIssued.Inc()
	shareCodesActive.Inc()
}

// RegisterShareCodeValidation records a share code validation attempt.
func RegisterShareCodeValidation(valid bool) {
	if valid {
		shareCodesValidated.WithLabelValues("valid").Inc()
	} else {
		shareCodesValidated.WithLabelValues("invalid").Inc()
	}
}

// ---------------------------------------------------------------------------
// Endpoint Registry metrics
// ---------------------------------------------------------------------------

var (
	endpointRegistry503 = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "endpoint_registry_503_total",
			Help:      "Total 503 Service Unavailable responses for registered-but-offline agents.",
		},
	)
	endpointRegistry404 = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "endpoint_registry_404_total",
			Help:      "Total 404 Not Found responses for unknown agents.",
		},
	)
)

// RegisterEndpointRegistry503 records a 503 for a registered but offline agent.
func RegisterEndpointRegistry503() {
	endpointRegistry503.Inc()
}

// IncrEndpointRegistry404 records a 404 for an unknown agent.
func IncrEndpointRegistry404() {
	endpointRegistry404.Inc()
}
