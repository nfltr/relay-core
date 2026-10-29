// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/nfltr/relay-core/core/session"
	"github.com/prometheus/client_golang/prometheus"
)

// TestFrontCountsUnansweredRequests: a request no agent answered (a share
// refused, rate limited, an agent offline: no metadata header) counts in
// requests_total with its status, and is not an ERROR line per request. It
// was neither counted nor logged with its status (found by a network
// fault test).
func TestFrontCountsUnansweredRequests(t *testing.T) {
	var logs bytes.Buffer
	ing := newTestIngress(&session.Core{})
	ing.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests, http.StatusBadGateway} {
		before := requestsTotal(t, "DELETE", status)
		ing.front(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })).
			ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodDelete, "/", nil))
		if got := requestsTotal(t, "DELETE", status) - before; got != 1 {
			t.Errorf("status %d: requests_total grew by %v, want 1", status, got)
		}
	}
	if strings.Contains(logs.String(), `"level":"ERROR"`) {
		t.Errorf("an unanswered request logged an error:\n%s", logs.String())
	}
}

// requestsTotal is rpc_server_proxy_requests_total{method, status}, summed
// over api_type.
func requestsTotal(t *testing.T, method string, status int) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var n float64
	for _, mf := range mfs {
		if mf.GetName() != "rpc_server_proxy_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["method"] == method && labels["status"] == strconv.Itoa(status) {
				n += m.GetCounter().GetValue()
			}
		}
	}
	return n
}
