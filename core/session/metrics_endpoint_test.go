// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// relayMetricsTestHandler is what the metrics port serves (serveMetrics).
func relayMetricsTestHandler() http.Handler {
	return promhttp.Handler()
}

func TestMetricsEndpointReturns200(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	relayMetricsTestHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") || !strings.Contains(ct, "version=0.0.4") {
		t.Fatalf("Content-Type = %q, want text/plain with Prometheus 0.0.4 version", ct)
	}
}
