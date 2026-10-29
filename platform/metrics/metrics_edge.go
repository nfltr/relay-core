// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// --- Edge metrics: what the relay does itself with no CDN in front ---

var (
	uploadRefusals = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "upload_refusals_total",
			Help:      "Request bodies refused by the upload limits (reason: per_ip, buffer_full).",
		},
		[]string{"reason"},
	)
	uploadBufferReserved = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: proxyNamespace,
			Name:      "upload_buffer_reserved_bytes",
			Help:      "Upload bytes reserved by bodies in flight (bounded by http.server.uploads.buffer_bytes).",
		},
	)
	tlsCertExpiry = auto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: proxyNamespace,
			Name:      "tls_certificate_expiry_timestamp_seconds",
			Help:      "Unix time at which the relay's certificate for a name expires.",
		},
		[]string{"name"},
	)
	tlsCertRenewals = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "tls_certificate_renewals_total",
			Help:      "Certificate issuance/renewal attempts by name and result (ok, error).",
		},
		[]string{"name", "result"},
	)
	httpRequestLabels = []string{"route", "method", "status_class"}
	httpRequests      = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: proxyNamespace,
			Name:      "http_requests_total",
			Help:      "HTTP requests on the relay's public listeners by route pattern (never the raw path; proxy: dispatched to an agent; unmatched: any other path), method and status class.",
		},
		httpRequestLabels,
	)
	httpRequestDuration = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: proxyNamespace,
			Name:      "http_request_duration_seconds",
			Help:      "Time to serve an HTTP request on the relay's public listeners, labelled like http_requests_total.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
		},
		httpRequestLabels,
	)
)

// ObserveHTTPRequest counts a served request and its duration. The caller
// bounds route and statusClass (route patterns, status classes); method is
// bounded here (httpMethod).
func ObserveHTTPRequest(route, method, statusClass string, d time.Duration) {
	method = httpMethod(method)
	httpRequests.WithLabelValues(route, method, statusClass).Inc()
	httpRequestDuration.WithLabelValues(route, method, statusClass).Observe(d.Seconds())
}

// IncrUploadRefusals counts a refused request body. allow:wrapper
func IncrUploadRefusals(reason string) { uploadRefusals.WithLabelValues(reason).Inc() }

// SetUploadBufferReservedBytes sets the reserved upload bytes. allow:wrapper
func SetUploadBufferReservedBytes(n int64) { uploadBufferReserved.Set(float64(n)) }

// SetTLSCertExpiry records when the certificate for name expires. allow:wrapper
func SetTLSCertExpiry(name string, unixSeconds int64) {
	tlsCertExpiry.WithLabelValues(name).Set(float64(unixSeconds))
}

// IncrTLSCertRenewals counts an issuance/renewal attempt ("ok" or "error"). allow:wrapper
func IncrTLSCertRenewals(name, result string) { tlsCertRenewals.WithLabelValues(name, result).Inc() }
