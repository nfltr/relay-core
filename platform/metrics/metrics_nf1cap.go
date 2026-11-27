// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package metrics

import "github.com/prometheus/client_golang/prometheus"

// The owner's daily cap on an nf1 address's relayed bytes (core/nf1cap;
// nfltr docs/decisions/nf1-relay-cap-2026-11-25.md): never by address or
// agent (unbounded), the owner's view is nf1cap.Registry.Usage.

var (
	nf1CapRelayedBytes = auto.NewCounter(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "nf1_relay_cap_bytes_total",
		Help:      "Relayed bytes of nf1 sessions counted against their address's daily cap (clients outside the owner's account), both ways.",
	})
	nf1CapRefusals = auto.NewCounterVec(prometheus.CounterOpts{
		Namespace: dispatcherNamespace,
		Name:      "nf1_relay_cap_refused_total",
		Help:      "nf1 sessions refused (at: open) or cut (at: cut) with ERROR_REASON_LIMIT_EXCEEDED at their address's daily cap.",
	}, []string{"at"})
)

// NF1CapRefused's at.
const (
	NF1CapAtOpen = "open"
	NF1CapCut    = "cut"
)

// NF1CapRelayed counts n bytes counted against an address's cap.
func NF1CapRelayed(n int) { nf1CapRelayedBytes.Add(float64(n)) }

// NF1CapRefused counts a refusal or a cut at an address's cap.
func NF1CapRefused(at string) { nf1CapRefusals.WithLabelValues(at).Inc() }
