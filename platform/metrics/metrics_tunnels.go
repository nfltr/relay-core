// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// ---------------------------------------------------------------------------
// Connect sessions
// ---------------------------------------------------------------------------

var connectSessions = auto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "connect_sessions_total",
		Help:      "Opens of /connect sessions (data connections, and released clients' HTTP/2, WebSocket and hijacked tunnels), by end-to-end encryption (true, false) and result (open, or the refusal's ErrorReason).",
	},
	[]string{"e2ee", "result"},
)

// RecordConnectSession counts one open of a /connect session on a data
// connection by its end-to-end encryption and result ("open" or the
// answer's reason, a bounded enum name).
func RecordConnectSession(e2ee bool, result string) {
	connectSessions.WithLabelValues(strconv.FormatBool(e2ee), result).Inc()
	if result != "open" { // an open counts as it starts (TransferBegin(PathConnect))
		TransferOpened(PathConnect, result)
	}
}
