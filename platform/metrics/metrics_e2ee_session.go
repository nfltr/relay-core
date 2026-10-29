// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// E2EE — TLS Passthrough & WireGuard VPN metrics
// ---------------------------------------------------------------------------

var (
	wgIPAllocations = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "wg_ip_allocations_total",
			Help:      "Total WireGuard IP address allocations.",
		},
	)
)

// IncrWGIPAllocations records a WireGuard IP allocation.
func IncrWGIPAllocations() {
	wgIPAllocations.Inc()
}
