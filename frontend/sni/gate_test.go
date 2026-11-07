// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sni

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/sharestore"
	"github.com/prometheus/client_golang/prometheus"
)

// sniDurationSum is the sum of the sni path's session lifetimes observed.
func sniDurationSum(t *testing.T) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "nfltr_relay_transfer_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "path" && l.GetValue() == "sni" {
					return m.GetHistogram().GetSampleSum()
				}
			}
		}
	}
	return 0
}

// TestRouteE2EESessionLifetimeStartsAtRouting: an SNI session's lifetime
// is timed from its routing (TransferBegin), not from the accept that
// preceded the ClientHello's peek (which can wait on a slow client).
func TestRouteE2EESessionLifetimeStartsAtRouting(t *testing.T) {
	store := sharestore.NewInMemory(time.Minute)
	defer store.Close()
	code := store.Issue("lifetime-agent", time.Minute, sharestore.Policy{})
	logger := slog.New(slog.DiscardHandler)
	d := session.NewTCPDispatcher(logger, nil, directory.NewNoOpInventory())
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // no agent: the route ends at once
	a, b := net.Pipe()
	defer b.Close() //nolint:errcheck

	before := sniDurationSum(t)
	routeE2EE(ctx, newResolver("e2ee.test.local", grantsOf(store)), d, logger, a, code+".e2ee.test.local", nil)
	if got := sniDurationSum(t) - before; got <= 0 || got >= 60 {
		t.Fatalf("sni lifetime observed %vs, want the routed session's (well under a minute)", got)
	}
}

// A refused E2EE connection gets a TLS alert record, then EOF: the client
// reports an alert, not a reset.
func TestRefuseTLSSendsAnAlert(t *testing.T) {
	a, b := net.Pipe()
	go refuseTLS(a)
	got := make([]byte, 16)
	n, _ := io.ReadAtLeast(b, got, len(alertAccessDenied))
	if string(got[:n]) != string(alertAccessDenied) {
		t.Fatalf("read % x, want the access_denied alert % x", got[:n], alertAccessDenied)
	}
	if _, err := b.Read(got); err != io.EOF {
		t.Fatalf("after the alert: %v, want EOF", err)
	}
}
