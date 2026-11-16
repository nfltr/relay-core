// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// nf1 addresses served with no account (core/nf1anon; nfltr
// docs/decisions/nf1-anon-2026-11-16.md): counts and bytes by bounded
// reasons, never by agent or source (an anonymous agent id is a fresh
// address's).

var (
	nf1AnonRegistrations = auto.NewGauge(prometheus.GaugeOpts{
		Namespace: dispatcherNamespace,
		Name:      "nf1_anon_registrations",
		Help:      "Anonymous nf1 registrations open on this replica (agents with no credential serving only capability addresses).",
	})
	nf1AnonRegistrationEvents = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "nf1_anon_registration_events_total",
		Help:      "Anonymous nf1 registrations by event: registered, refused (over a cap: limit; a proof that does not hold: proof), expired (its lifetime reached).",
	}, []string{"event"})
	nf1AnonRelayedBytes = auto.NewCounter(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "nf1_anon_relayed_bytes_total",
		Help:      "Bytes the relayed legs of anonymous nf1 sessions carried, both ways (signalling: the bulk goes direct).",
	})
	nf1AnonCuts = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "nf1_anon_cut_total",
		Help:      "Relayed legs of anonymous nf1 sessions the relay cut with ERROR_REASON_ACCOUNT_REQUIRED, by reason: bytes (the leg's budget or the source's bucket), time (the leg's time limit).",
	}, []string{"reason"})
)

// NF1AnonRegistration events.
const (
	NF1AnonRegistered   = "registered"
	NF1AnonRefusedLimit = "refused_limit"
	NF1AnonRefusedProof = "refused_proof"
	NF1AnonExpired      = "expired"
	NF1AnonCutBytes     = "bytes"
	NF1AnonCutTime      = "time"
)

// NF1AnonRegistration counts an anonymous registration's event; a
// registered one is open until NF1AnonLeft.
func NF1AnonRegistration(event string) {
	nf1AnonRegistrationEvents.WithLabelValues(event).Inc()
	if event == NF1AnonRegistered {
		nf1AnonRegistrations.Inc()
	}
}

// NF1AnonLeft counts an anonymous registration's end (its connection's).
func NF1AnonLeft() { nf1AnonRegistrations.Dec() }

// NF1AnonRelayed counts n bytes an anonymous session's relayed leg carried.
func NF1AnonRelayed(n int) { nf1AnonRelayedBytes.Add(float64(n)) }

// NF1AnonCut counts a cut of an anonymous session's relayed leg by reason
// (NF1AnonCutBytes, NF1AnonCutTime).
func NF1AnonCut(reason string) { nf1AnonCuts.WithLabelValues(reason).Inc() }
