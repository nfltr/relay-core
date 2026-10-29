// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Sessions the relay forwards as a hop of a link chain (link.Forward,
// relay adapters session_chain.go): a client's /connect session, and a
// session another replica forwards here. The trace id that joins one
// session's lines across hops is in the logs, never a label.
var (
	chainAnswers = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "chain_answers_total",
		Help:      "Answers to sessions the relay forwards, by path (connect: a client's data connection session; replica: one another replica forwarded), result (accepted, refused), code (the refusal's: an ErrorReason name, or link's own: draining, conn_lost, no_route, timeout, hop_limit, ...) and hops (how far beyond this relay it was refused: 0 this relay, 1 its next hop, ...).",
	}, []string{"path", "result", "code", "hops"})
	chainAnswerSeconds = auto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: dispatcherNamespace,
		Name:      "chain_answer_seconds",
		Help:      "Time from a forwarded session's open here to its answer from the rest of the path (the agent reaching its target, or a refusal), by path.",
		Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 16),
	}, []string{"path"})
	chainAborts = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "chain_aborts_total",
		Help:      "Forwarded sessions that ended in an abort after they opened, by path, code (link's or an ErrorReason name) and hops (how far beyond this relay the abort came from).",
	}, []string{"path", "code", "hops"})
)

// RecordChainAnswer counts one forwarded session's answer: refused with
// code from hops beyond this relay, or accepted (code "").
func RecordChainAnswer(path string, refused bool, code string, hops uint8, latency time.Duration) {
	result := "accepted"
	if refused {
		result = "refused"
	}
	chainAnswers.WithLabelValues(path, result, code, hopsLabel(hops)).Inc()
	chainAnswerSeconds.WithLabelValues(path).Observe(latency.Seconds())
	switch path {
	case "connect":
		ObserveTransferOpen(PathConnect, latency)
	case "replica":
		ObserveTransferOpen(PathReplica, latency)
	}
}

// RecordChainAbort counts one forwarded session's abort. allow:wrapper
func RecordChainAbort(path, code string, hops uint8) {
	chainAborts.WithLabelValues(path, code, hopsLabel(hops)).Inc()
}

// hopsLabel bounds the label's values: a path is at most link's default
// MaxHops (8) long, a peer's larger count is "8".
func hopsLabel(hops uint8) string { return strconv.Itoa(int(min(hops, 8))) }
