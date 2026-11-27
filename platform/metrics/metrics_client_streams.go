// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// clientStreams counts the streams open on this relay by the client program
// that opened them (agentwire identity): which client versions are still
// connected before a protocol cut.
var clientStreams = auto.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: dispatcherNamespace,
		Name:      "client_streams",
		Help:      "Open gRPC streams by method and the client name and version that opened them.",
	},
	[]string{"method", "client_name", "client_version"},
)

// maxClientVersions bounds the client_name/client_version pairs the gauge
// keeps: both are client input. Later pairs count as "other".
const maxClientVersions = 256

var clientVersions = struct {
	sync.Mutex
	seen map[[2]string]bool
}{seen: map[[2]string]bool{}}

// TrackClientStream counts one open stream of method by client name and
// version ("" reads as "unknown"); the returned func ends it.
func TrackClientStream(method, name, version string) func() {
	if name == "" {
		name = "unknown"
	}
	if version == "" {
		version = "unknown"
	}
	pair := [2]string{name, version}
	clientVersions.Lock()
	if !clientVersions.seen[pair] {
		if len(clientVersions.seen) >= maxClientVersions {
			pair = [2]string{"other", "other"}
		} else {
			clientVersions.seen[pair] = true
		}
	}
	clientVersions.Unlock()
	g := clientStreams.WithLabelValues(method, pair[0], pair[1])
	g.Inc()
	return g.Dec
}

var protocolRefusals = auto.NewCounter(prometheus.CounterOpts{
	Namespace: dispatcherNamespace,
	Name:      "protocol_refusals_total",
	Help:      "Calls refused for carrying no protocol v1 Hello (clients older than the hard cut).",
})

// RecordProtocolRefusal counts one call refused for its protocol (no v1
// Hello). allow:wrapper
func RecordProtocolRefusal() { protocolRefusals.Inc() }
