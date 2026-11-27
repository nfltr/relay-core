// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// --- Replica link (link v2 between relay replicas) ---

var (
	peerLinkSessions = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_sessions_total",
			Help:      "Sessions forwarded to another replica, by kind (tcp, relay, http) and path (link: on the replica link; none: refused, the peer cannot be reached on the link or refused the session; unreachable: refused without a dial, the peer having not answered one within the dial timeout a moment before).",
		},
		[]string{"kind", "path"},
	)

	peerLinkTransport = auto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_transport",
			Help:      "The replica link's transport (peer_link.transport: mtls or plaintext): 1 for the one this replica runs.",
		},
		[]string{"transport"},
	)

	peerLinkConns = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_conns_total",
			Help:      "Replica link connection events: up, down, dead_peer (the link's liveness), goaway (the peer drains), refused (authentication), transport_mismatch (the peer runs the other peer_link.transport), unreachable (a dial got no answer within the dial timeout: sessions toward that peer are refused without a dial for as long), accept_error (the peer link port's Accept failed other than closed, e.g. EMFILE: retried after a backoff).",
		},
		[]string{"event"},
	)

	// Per peer replica (label replica: its pod address; bounded by the
	// replicas this one talked to, each series removed once the peer has
	// had no connection for the pool idle time).
	peerLinkPeerConns = auto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_conns",
			Help:      "Open replica link connections with each peer replica (both directions), by the peer's pod address.",
		},
		[]string{"replica"},
	)

	peerLinkPeerConnEvents = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_conn_events_total",
			Help:      "Replica link connection events per peer replica: up, down, dead_peer (the link's liveness), goaway (the peer drains).",
		},
		[]string{"replica", "event"},
	)

	peerLinkPeerRTT = auto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_ping_rtt_seconds",
			Help:      "Round trip of the replica link's liveness pings, per peer replica.",
			Buckets:   []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"replica"},
	)

	peerLinkPeerCreditWaits = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_credit_waits_total",
			Help:      "Times a sender on the replica link waited for a session's credit (the receiving side's window was full), per peer replica.",
		},
		[]string{"replica"},
	)

	peerLinkPeerBytes = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_bytes_total",
			Help:      "Bytes this replica wrote to (direction sent) and read from (received) its replica link connections with each peer replica, counted on the TCP connection: TLS records, link frames and handshakes included.",
		},
		[]string{"replica", "direction"},
	)

	peerLinkPeerSessions = auto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_sessions",
			Help:      "Open replica link sessions with each peer replica: those this replica opened and those the peer opened.",
		},
		[]string{"replica"},
	)

	peerLinkPeerCreditWaitSeconds = auto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: dispatcherNamespace,
			Name:      "peer_link_peer_credit_wait_seconds_total",
			Help:      "Time senders on the replica link waited for credit, per peer replica.",
		},
		[]string{"replica"},
	)
)

// SetPeerLinkPeerConns records how many replica link connections are open
// with peer.
func SetPeerLinkPeerConns(peer string, n int) {
	peerLinkPeerConns.WithLabelValues(peer).Set(float64(n))
}

// RegisterPeerLinkPeerConn counts a replica link connection event with peer.
func RegisterPeerLinkPeerConn(peer, event string) {
	peerLinkPeerConnEvents.WithLabelValues(peer, event).Inc()
}

// ObservePeerLinkPing records a liveness ping's round trip to peer.
func ObservePeerLinkPing(peer string, d time.Duration) {
	peerLinkPeerRTT.WithLabelValues(peer).Observe(d.Seconds())
}

// RegisterPeerLinkCreditWait counts a wait for credit toward peer.
func RegisterPeerLinkCreditWait(peer string, d time.Duration) {
	peerLinkPeerCreditWaits.WithLabelValues(peer).Inc()
	peerLinkPeerCreditWaitSeconds.WithLabelValues(peer).Add(d.Seconds())
}

// PeerLinkPeerBytes returns peer's byte counters, sent and received: a
// connection adds to them without a label lookup per read or write.
func PeerLinkPeerBytes(peer string) (sent, received prometheus.Counter) {
	return peerLinkPeerBytes.WithLabelValues(peer, "sent"), peerLinkPeerBytes.WithLabelValues(peer, "received")
}

// AddPeerLinkPeerSessions moves peer's open sessions by delta.
func AddPeerLinkPeerSessions(peer string, delta int) {
	peerLinkPeerSessions.WithLabelValues(peer).Add(float64(delta))
}

// DeletePeerLinkPeer removes peer's series (a replica gone for good).
func DeletePeerLinkPeer(peer string) {
	l := prometheus.Labels{"replica": peer}
	peerLinkPeerConns.DeletePartialMatch(l)
	peerLinkPeerConnEvents.DeletePartialMatch(l)
	peerLinkPeerRTT.DeletePartialMatch(l)
	peerLinkPeerCreditWaits.DeletePartialMatch(l)
	peerLinkPeerCreditWaitSeconds.DeletePartialMatch(l)
	peerLinkPeerBytes.DeletePartialMatch(l)
	peerLinkPeerSessions.DeletePartialMatch(l)
}

// RegisterPeerLinkSession counts a session forwarded to another replica.
func RegisterPeerLinkSession(kind, path string) {
	peerLinkSessions.WithLabelValues(kind, path).Inc()
}

// SetPeerLinkTransport records the replica link's transport.
func SetPeerLinkTransport(transport string) {
	peerLinkTransport.Reset()
	peerLinkTransport.WithLabelValues(transport).Set(1)
}

// RegisterPeerLinkConn counts a replica link connection event.
func RegisterPeerLinkConn(event string) {
	peerLinkConns.WithLabelValues(event).Inc()
}
