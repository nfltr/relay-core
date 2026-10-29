// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// TCP Tunnel metrics
// ---------------------------------------------------------------------------

var (
	tcpTunnelConnections = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "tcp_tunnel_connections_total",
			Help:      "Total TCP tunnel connections established.",
		},
		[]string{"path"}, // path: "local" or "cross-pod"
	)
	tcpTunnelActiveConnections = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "tcp_tunnel_active_connections",
			Help:      "Current number of active TCP tunnel connections.",
		},
	)
	tcpTunnelDuration = auto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "tcp_tunnel_duration_seconds",
			Help:      "Duration of TCP tunnel sessions in seconds.",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600},
		},
	)
	tcpTunnelErrors = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "tcp_tunnel_errors_total",
			Help:      "Total TCP tunnel errors.",
		},
		[]string{"error_type"}, // error_type: "dial", "relay", "bridge", "stalled" (a session reset: its client took none of its bytes)
	)
)

// RegisterTCPTunnelConnection records a new TCP tunnel session.
func RegisterTCPTunnelConnection(path string) {
	tcpTunnelConnections.WithLabelValues(path).Inc()
	tcpTunnelActiveConnections.Inc()
}

// DeregisterTCPTunnelConnection decrements the active connection gauge.
func DeregisterTCPTunnelConnection() {
	tcpTunnelActiveConnections.Dec()
}

// RegisterTCPTunnelDuration records the duration of a completed TCP tunnel session.
func RegisterTCPTunnelDuration(durationSec float64) {
	tcpTunnelDuration.Observe(durationSec)
}

// RegisterTCPTunnelError records a TCP tunnel error.
func RegisterTCPTunnelError(errorType string) {
	tcpTunnelErrors.WithLabelValues(errorType).Inc()
}

// ---------------------------------------------------------------------------
// E2EE SNI Router metrics
// ---------------------------------------------------------------------------

var (
	sniRouterConnections = auto.NewCounter(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "sni_router_connections_total",
			Help:      "Total E2EE TLS passthrough connections routed via SNI.",
		},
	)
	sniRouterActiveConnections = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "sni_router_active_connections",
			Help:      "Current number of active E2EE TLS passthrough connections.",
		},
	)
	sniRouterDuration = auto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "sni_router_duration_seconds",
			Help:      "Duration of E2EE TLS passthrough sessions in seconds.",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600},
		},
	)
	sniRouterErrors = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "sni_router_errors_total",
			Help:      "Total SNI router errors.",
		},
		[]string{"error_type"}, // error_type: "parse", "resolve", "share_ip_refused", "share_policy_unenforced"
	)
)

// RegisterSNIRouterConnection records a new E2EE TLS passthrough connection.
func RegisterSNIRouterConnection() {
	sniRouterConnections.Inc()
	sniRouterActiveConnections.Inc()
	transferStarted(PathSNI)
}

// DeregisterSNIRouterConnection decrements the active E2EE connection gauge.
func DeregisterSNIRouterConnection() {
	sniRouterActiveConnections.Dec()
	transfers.activeN[PathSNI].Add(-1)
}

// RegisterSNIRouterDuration records the duration of an E2EE TLS passthrough session.
func RegisterSNIRouterDuration(durationSec float64) {
	sniRouterDuration.Observe(durationSec)
	PathSNI.series().duration.Observe(durationSec)
}

// RegisterSNIRouterError records an SNI router error.
func RegisterSNIRouterError(errorType string) {
	sniRouterErrors.WithLabelValues(errorType).Inc()
	if reason, refused := sniRefusals[errorType]; refused {
		TransferOpened(PathSNI, reason)
	}
}

// sniRefusals are the SNI router's errors before a connection opens, by
// the transfer result they count as.
var sniRefusals = map[string]string{
	"share_store":      "ERROR_REASON_DIRECTORY_UNAVAILABLE",
	"resolve":          "ERROR_REASON_AGENT_NOT_FOUND",
	"share_ip_refused": "ERROR_REASON_SHARE_IP_REFUSED",
	"parse":            "ERROR_REASON_PROTOCOL_UNSUPPORTED",
}

// ---------------------------------------------------------------------------
// SSH Proxy metrics
// ---------------------------------------------------------------------------

var (
	sshProxyConnections = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "ssh_proxy_connections_total",
			Help:      "Total SSH proxy connections established.",
		},
		[]string{"path"}, // path: "local" or "cross-pod"
	)
	connectSessions = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "connect_sessions_total",
			Help:      "Opens of /connect sessions (data connections, and released clients' HTTP/2, WebSocket and hijacked tunnels), by end-to-end encryption (true, false) and result (open, or the refusal's ErrorReason).",
		},
		[]string{"e2ee", "result"},
	)
	sshProxyActiveConnections = auto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "ssh_proxy_active_connections",
			Help:      "Current number of active SSH proxy connections.",
		},
	)
	sshProxyDuration = auto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "ssh_proxy_duration_seconds",
			Help:      "Duration of SSH proxy sessions in seconds.",
			Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		},
	)
	sshProxyErrors = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "ssh_proxy_errors_total",
			Help:      "Total SSH proxy errors.",
		},
		[]string{"error_type"}, // error_type: "auth", "dial", "hijack"
	)
)

// RecordConnectSession counts one open of a /connect session on a data
// connection by its end-to-end encryption and result ("open" or the
// answer's reason, a bounded enum name).
func RecordConnectSession(e2ee bool, result string) {
	connectSessions.WithLabelValues(strconv.FormatBool(e2ee), result).Inc()
	if result != "open" { // an open counts as it starts (RegisterSSHProxyConnection)
		TransferOpened(PathConnect, result)
	}
}

// RegisterSSHProxyConnection records a new SSH proxy session.
func RegisterSSHProxyConnection(path string) {
	sshProxyConnections.WithLabelValues(path).Inc()
	sshProxyActiveConnections.Inc()
	transferStarted(PathConnect) // every connect session counts here, ssh-proxy's too
}

// DeregisterSSHProxyConnection decrements the active SSH proxy gauge.
func DeregisterSSHProxyConnection() {
	sshProxyActiveConnections.Dec()
	transfers.activeN[PathConnect].Add(-1)
}

// RegisterSSHProxyDuration records completed SSH proxy session duration.
func RegisterSSHProxyDuration(durationSec float64) {
	sshProxyDuration.Observe(durationSec)
	PathConnect.series().duration.Observe(durationSec)
}

// RegisterSSHProxyError records an SSH proxy error.
func RegisterSSHProxyError(errorType string) {
	sshProxyErrors.WithLabelValues(errorType).Inc()
}
