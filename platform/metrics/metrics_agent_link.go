// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// The agent link (relay adapters agent_link.go): an agent's sessions on
// peerlink over link/TCP or QUIC.
var (
	agentLinkConnections = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "agent_link_connections_total",
		Help:      "Agent-link connections, by transport (tcp, quic) and result (bound; refused: an unknown token or one past its connections; silent: it ended silent past its liveness timeout, which ends its primary stream).",
	}, []string{"transport", "result"})
	agentLinkSessions = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "agent_link_sessions_total",
		Help:      "Sessions the relay opened on agent links, by transport (tcp, quic) and kind (tcp, e2ee, http).",
	}, []string{"transport", "kind"})
	agentLinkOpenSessions = auto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: dispatcherNamespace,
		Name:      "agent_link_open_sessions",
		Help:      "Sessions open on agent links, by transport (tcp, quic).",
	}, []string{"transport"})
	agentLinkBytes = register(&linkBytes{
		desc: prometheus.NewDesc(prometheus.BuildFQName(dispatcherNamespace, "", "agent_link_bytes_total"),
			"Bytes on agent-link connections, by transport and direction (sent: to the agent; received: from it). tcp: counted on the connection after its magic (link frames and handshake; inside the gRPC port's TLS when it has one); quic: quic-go's count of the connection's packets (retransmissions included, UDP headers not).",
			[]string{"transport", "direction"}, nil),
		quic: map[*func() (uint64, uint64)]struct{}{},
	})
	agentLinkTransports = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "agent_link_transport_total",
		Help:      "Primary TCP streams that asked for the agent link, by the transport asked (tcp, quic, auto) and the one bound first (tcp, quic; grpc: none, its link did not connect or no session asked for it).",
	}, []string{"requested", "bound"})
)

// RecordAgentLinkConnection counts one agent-link connection. allow:wrapper
func RecordAgentLinkConnection(transport, result string) {
	agentLinkConnections.WithLabelValues(transport, result).Inc()
}

// RecordAgentLinkSession counts one session opened on an agent link. allow:wrapper
func RecordAgentLinkSession(transport, kind string) {
	agentLinkSessions.WithLabelValues(transport, kind).Inc()
}

// RecordAgentLinkTransport counts a primary that asked for the agent link
// by what it bound. requested is the agent's value: anything but tcp,
// quic or auto counts as "other". allow:wrapper
func RecordAgentLinkTransport(requested, bound string) {
	switch requested {
	case "tcp", "quic", "auto":
	default:
		requested = "other"
	}
	agentLinkTransports.WithLabelValues(requested, bound).Inc()
}

// AddAgentLinkOpenSessions moves the open agent-link sessions of
// transport by delta. allow:wrapper
func AddAgentLinkOpenSessions(transport string, delta int) {
	agentLinkOpenSessions.WithLabelValues(transport).Add(float64(delta))
}

// AddAgentLinkTCPBytes counts bytes an agent-link TCP connection wrote
// (sent) and read (received).
func AddAgentLinkTCPBytes(sent, received int) {
	if sent > 0 {
		agentLinkBytes.tcp[0].Add(int64(sent))
	}
	if received > 0 {
		agentLinkBytes.tcp[1].Add(int64(received))
	}
}

// TrackAgentLinkQUIC counts a QUIC agent-link connection's bytes: stats,
// its totals sent and received, is read at each scrape until done, which
// adds its last totals in for good.
func TrackAgentLinkQUIC(stats func() (sent, received uint64)) (done func()) {
	b := agentLinkBytes
	key := &stats
	b.mu.Lock()
	b.quic[key] = struct{}{}
	b.mu.Unlock()
	return sync.OnceFunc(func() {
		s, r := stats()
		b.mu.Lock()
		delete(b.quic, key)
		b.quicDone[0] += float64(s)
		b.quicDone[1] += float64(r)
		b.mu.Unlock()
	})
}

// linkBytes is agent_link_bytes_total: TCP connections add as they read
// and write (atomics, no label lookup); QUIC connections are read when
// scraped (quic-go counts them).
type linkBytes struct {
	desc *prometheus.Desc
	tcp  [2]atomic.Int64 // sent, received

	mu       sync.Mutex
	quic     map[*func() (uint64, uint64)]struct{} // open connections' totals
	quicDone [2]float64                            // closed connections', sent and received
}

func (b *linkBytes) Describe(ch chan<- *prometheus.Desc) { ch <- b.desc }

func (b *linkBytes) Collect(ch chan<- prometheus.Metric) {
	b.mu.Lock()
	q := b.quicDone
	for stats := range b.quic {
		s, r := (*stats)()
		q[0] += float64(s)
		q[1] += float64(r)
	}
	b.mu.Unlock()
	for i, dir := range [2]string{"sent", "received"} {
		ch <- prometheus.MustNewConstMetric(b.desc, prometheus.CounterValue, float64(b.tcp[i].Load()), "tcp", dir)
		ch <- prometheus.MustNewConstMetric(b.desc, prometheus.CounterValue, q[i], "quic", dir)
	}
}
