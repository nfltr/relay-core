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

var rateLimitEvictions = auto.NewCounter(
	prometheus.CounterOpts{
		Namespace: proxyNamespace,
		Name:      "ratelimit_evictions_total",
		Help:      "Rate-limit buckets evicted because a limiter held its cap of keys (audit F-26); the evicted key starts again with a full burst.",
	},
)

// IncrRateLimitEvictions counts a bucket a full limiter evicted.
func IncrRateLimitEvictions() { rateLimitEvictions.Inc() }
