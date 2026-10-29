// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/link"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/prometheus/client_golang/prometheus"
)

// The transfer families (nfltr_relay_transfer_*): one shape for every data
// path of the relay, labelled by a closed Path. A session counts on its
// frontend's path (http_*, sni, connect, ssh, a2a, ...) and again on the
// agent transport it rides (tcp_link, tcp_quic, tcp_dataconn, tcp_grpc), so
// a sum over every path counts it twice; a path's own series is what a
// panel shows. Hot paths add to atomics or to series resolved once per
// path: nothing allocates per session or per byte, nothing runs between
// scrapes (TestTransferHotPathAllocs).

// Path is a data path: the `path` label of the transfer families.
type Path uint8

// The paths. Append only: the guard in nfltr reads PathNames.
const (
	PathHTTPLink    Path = iota // an HTTP request on the agent link
	PathHTTPGRPC                // an HTTP request on the agent's gRPC stream
	PathSNI                     // an E2EE share's TLS connection on the SNI port
	PathConnect                 // a connect client's session (nfltr dial, connect:, tcp-connect)
	PathSSH                     // nfltr ssh-proxy / shell
	PathTCPLink                 // a session on the agent link over link/TCP
	PathTCPQUIC                 // a session on the agent link over QUIC
	PathTCPDataConn             // a session on a link data connection
	PathTCPGRPC                 // a session on the agent's gRPC frame streams
	PathA2A                     // an agent-to-agent call
	PathReplica                 // a session another replica carries
	PathWG                      // WireGuard
	PathP2PRelay                // p2p relayed through the relay
	PathMailbox                 // the mailbox service
	PathBlob                    // the blob service
	PathAccounts                // the accounts service
	numPaths
)

var pathNames = [numPaths]string{
	"http_link", "http_grpc", "sni", "connect", "ssh", "tcp_link", "tcp_quic", "tcp_dataconn",
	"tcp_grpc", "a2a", "replica", "wg", "p2p_relay", "mailbox", "blob", "accounts",
}

// String is the path's label value.
func (p Path) String() string { return pathNames[p] }

// PathNames is every Path's label value, in order.
func PathNames() []string { return pathNames[:] }

// The errors after open (TransferError's reason).
const (
	TransferDeadPeer = "dead_peer" // the peer went silent past its liveness
	TransferStalled  = "stalled"   // a reader took none of its bytes past the stall bound
	TransferReset    = "reset"     // a side reset the session
	TransferTimeout  = "timeout"
	TransferDrain    = "drain" // a drain's deadline cut it
	TransferLimit    = "limit" // a limit of the relay's ended it
	// "abort": a link ABORT, its code in the code label (TransferAbort).
)

var transferReasons = []string{TransferDeadPeer, TransferStalled, TransferReset, TransferTimeout, TransferDrain, TransferLimit}

// abortCodes names link's abort codes (link.Code*) for the code label:
// app is any application code, other one link may add later.
var abortCodes = [...]string{"unspecified", "refused", "draining", "hop_limit", "no_route", "conn_lost", "timeout", "drain_deadline"}

func abortCode(code uint32) string {
	switch {
	case int(code) < len(abortCodes):
		return abortCodes[code]
	case code >= link.CodeApp:
		return "app"
	}
	return "other"
}

// SecondsBuckets: 1 ms to 60 s, 12 buckets, the one bucket list of the
// relay's new seconds histograms (the transfer families, gRPC handling time).
var SecondsBuckets = prometheus.ExponentialBucketsRange(0.001, 60, 12)

var (
	transferOpens = auto.NewCounterVec(prometheus.CounterOpts{
		Name: "nfltr_relay_transfer_opens_total",
		Help: "Sessions, requests and streams opened on a data path, by path and result (ok, or the refusal's ErrorReason name; other).",
	}, []string{"path", "result"})
	transferErrors = auto.NewCounterVec(prometheus.CounterOpts{
		Name: "nfltr_relay_transfer_errors_total",
		Help: "Failures after open, by path, reason (dead_peer, stalled, reset, abort, timeout, drain, limit) and code (a link ABORT's: refused, draining, no_route, conn_lost, timeout, ..., app; none for the other reasons).",
	}, []string{"path", "reason", "code"})
	transferOpenSeconds = auto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nfltr_relay_transfer_open_seconds",
		Help:    "Time from a session's open here to the agent's answer, by path.",
		Buckets: SecondsBuckets,
	}, []string{"path"})
	transferFirstByteSeconds = auto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nfltr_relay_transfer_first_byte_seconds",
		Help:    "Time from a session's open to the first byte from the agent, by path.",
		Buckets: SecondsBuckets,
	}, []string{"path"})
	transferDurationSeconds = auto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nfltr_relay_transfer_duration_seconds",
		Help:    "Session lifetimes, by path.",
		Buckets: SecondsBuckets,
	}, []string{"path"})
	transfers = register(&transferCollector{
		active: prometheus.NewDesc("nfltr_relay_transfer_active", "Sessions open now, by path.", []string{"path"}, nil),
		bytes: prometheus.NewDesc("nfltr_relay_transfer_bytes_total",
			"Bytes on a data path while they flow, by path and direction (to_agent, from_agent). tcp_link, tcp_quic and tcp_dataconn: the transport connection's bytes (framing included; QUIC: its packets'); tcp_grpc: session payloads.",
			[]string{"path", "direction"}, nil),
		stallSeconds: prometheus.NewDesc("nfltr_relay_transfer_stall_seconds_total", "Time senders waited for credit, by path.", []string{"path"}, nil),
		stalls:       prometheus.NewDesc("nfltr_relay_transfer_stalls_total", "Times senders waited for credit, by path.", []string{"path"}, nil),
		queued:       prometheus.NewDesc("nfltr_relay_transfer_queued_bytes", "Bytes received from the agent and not yet read by their sessions, by path.", []string{"path"}, nil),
		conns:        map[*func() link.ConnStats]Path{},
	})
)

// pathSeries is a path's series resolved once, so a hot path does no
// label lookup.
type pathSeries struct {
	once                      sync.Once
	ok                        prometheus.Counter
	open, firstByte, duration prometheus.Observer
}

var series [numPaths]pathSeries

func (p Path) series() *pathSeries {
	s := &series[p]
	s.once.Do(func() {
		name := pathNames[p]
		s.ok = transferOpens.WithLabelValues(name, "ok")
		s.open = transferOpenSeconds.WithLabelValues(name)
		s.firstByte = transferFirstByteSeconds.WithLabelValues(name)
		s.duration = transferDurationSeconds.WithLabelValues(name)
	})
	return s
}

// TransferOpened counts an open on p: result "ok", or the refusal's
// ErrorReason name (anything else counts as "other").
func TransferOpened(p Path, result string) {
	if result == "ok" {
		p.series().ok.Inc()
		return
	}
	if _, known := relayv1.ErrorReason_value[result]; !known {
		result = "other"
	}
	transferOpens.WithLabelValues(pathNames[p], result).Inc()
}

// TransferBegin counts an open on p that went through and the session as
// active until TransferEnd(p, began), began being what it returns.
func TransferBegin(p Path) (began time.Time) {
	transferStarted(p)
	return time.Now()
}

func transferStarted(p Path) {
	p.series().ok.Inc()
	transfers.activeN[p].Add(1)
}

// TransferEnd ends a session TransferBegin began: it is no longer active
// and its lifetime is observed.
func TransferEnd(p Path, began time.Time) {
	transfers.activeN[p].Add(-1)
	p.series().duration.Observe(time.Since(began).Seconds())
}

// ObserveTransferOpen observes a session's open-to-answer time on p.
func ObserveTransferOpen(p Path, d time.Duration) { p.series().open.Observe(d.Seconds()) }

// ObserveTransferFirstByte observes a session's open-to-first-byte time on p.
func ObserveTransferFirstByte(p Path, d time.Duration) {
	p.series().firstByte.Observe(d.Seconds())
}

// TransferError counts a failure after open on p, reason one of the
// Transfer* reasons (anything else counts as "other").
func TransferError(p Path, reason string) {
	known := false
	for _, r := range transferReasons {
		known = known || r == reason
	}
	if !known {
		reason = "other"
	}
	transferErrors.WithLabelValues(pathNames[p], reason, "none").Inc()
}

// TransferAbort counts a link ABORT on p by its code.
func TransferAbort(p Path, code uint32) {
	transferErrors.WithLabelValues(pathNames[p], "abort", abortCode(code)).Inc()
}

// AddTransferBytes counts bytes the relay moved on p itself (paths whose
// transport TrackTransferConn does not count).
func AddTransferBytes(p Path, toAgent, fromAgent int) {
	b := &transfers.bytesN[p]
	if toAgent > 0 {
		b[0].Add(uint64(toAgent))
	}
	if fromAgent > 0 {
		b[1].Add(uint64(fromAgent))
	}
}

// AddTransferStall counts a sender's wait for credit on p.
func AddTransferStall(p Path, d time.Duration) {
	transfers.stallsN[p].Add(1)
	transfers.stallNanos[p].Add(int64(d))
}

// TrackTransferConn counts a link connection's ConnStats on p, read at
// each scrape (BytesOut is to the agent, BytesIn from it) until done, which
// adds its last counters in for good, so p's counters never go back. A
// source that keeps its ended connections' counts itself (quiclink's
// Node.Stats) never calls done.
func TrackTransferConn(p Path, stats func() link.ConnStats) (done func()) {
	t := transfers
	key := &stats
	t.mu.Lock()
	t.conns[key] = p
	t.mu.Unlock()
	return sync.OnceFunc(func() {
		st := stats()
		st.QueuedBytes = 0 // a gauge: an ended connection holds nothing
		t.mu.Lock()
		delete(t.conns, key)
		t.ended[p] = sumStats(t.ended[p], st)
		t.mu.Unlock()
	})
}

func sumStats(a, b link.ConnStats) link.ConnStats {
	a.BytesIn += b.BytesIn
	a.BytesOut += b.BytesOut
	a.CreditWaits += b.CreditWaits
	a.CreditWait += b.CreditWait
	a.QueuedBytes += b.QueuedBytes
	return a
}

// TransferObserver is a link.Observer counting p's ABORTs and, when
// creditWaits is set, its senders' waits for credit (left out where
// TrackTransferConn reads them from the connection's ConnStats).
// Observers that need more embed it.
func TransferObserver(p Path, creditWaits bool) link.Observer {
	return transferObserver{p: p, creditWaits: creditWaits}
}

type transferObserver struct {
	link.NopObserver
	p           Path
	creditWaits bool
}

func (o transferObserver) CreditWait(d time.Duration) {
	if o.creditWaits {
		AddTransferStall(o.p, d)
	}
}

func (o transferObserver) SessionAbort(a link.Abort, _ bool) { TransferAbort(o.p, a.Code) }

// transferCollector serves the transfer families computed at scrape:
// atomics the hot paths add to, and the tracked connections' ConnStats.
type transferCollector struct {
	active, bytes, stallSeconds, stalls, queued *prometheus.Desc

	activeN    [numPaths]atomic.Int64
	bytesN     [numPaths][2]atomic.Uint64 // to_agent, from_agent
	stallsN    [numPaths]atomic.Uint64
	stallNanos [numPaths]atomic.Int64

	mu    sync.Mutex
	conns map[*func() link.ConnStats]Path
	ended [numPaths]link.ConnStats
}

func (c *transferCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.active, c.bytes, c.stallSeconds, c.stalls, c.queued} {
		ch <- d
	}
}

func (c *transferCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	st := c.ended
	for stats, p := range c.conns {
		st[p] = sumStats(st[p], (*stats)())
	}
	c.mu.Unlock()
	for p := range numPaths {
		name := pathNames[p]
		s := st[p]
		to := float64(c.bytesN[p][0].Load() + s.BytesOut)
		from := float64(c.bytesN[p][1].Load() + s.BytesIn)
		if p == PathTCPLink { // the agent link's TCP connections count as they read and write
			to += float64(agentLinkBytes.tcp[0].Load())
			from += float64(agentLinkBytes.tcp[1].Load())
		}
		waits := float64(c.stallsN[p].Load() + s.CreditWaits)
		waited := (time.Duration(c.stallNanos[p].Load()) + s.CreditWait).Seconds()
		ch <- prometheus.MustNewConstMetric(c.active, prometheus.GaugeValue, float64(c.activeN[p].Load()), name)
		ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.CounterValue, to, name, "to_agent")
		ch <- prometheus.MustNewConstMetric(c.bytes, prometheus.CounterValue, from, name, "from_agent")
		ch <- prometheus.MustNewConstMetric(c.stallSeconds, prometheus.CounterValue, waited, name)
		ch <- prometheus.MustNewConstMetric(c.stalls, prometheus.CounterValue, waits, name)
		ch <- prometheus.MustNewConstMetric(c.queued, prometheus.GaugeValue, float64(s.QueuedBytes), name)
	}
}
