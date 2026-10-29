// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// --- Rate limiting metrics ---

var (
	rateLimitRejections = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "ratelimit_rejections_total",
			Help:      "Total requests rejected by per-AgentID rate limiting.",
		},
	)
)

// IncrRateLimitRejections counts a request the rate limit rejected.
func IncrRateLimitRejections() {
	rateLimitRejections.Inc()
}
