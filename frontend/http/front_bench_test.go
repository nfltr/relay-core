// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
)

// BenchmarkFirstMiddleware measures the cost of the outermost middleware
// (security headers, UUID generation, timing, metrics recording).
func BenchmarkFirstMiddleware(b *testing.B) {
	ing := newTestIngress(&session.Core{})

	m := Metadata{
		AgentID:              "bench-agent-1",
		RequestAtHTTPServer:  time.Now().UnixMilli(),
		RequestAtDispatcher:  time.Now().UnixMilli(),
		RequestAtAgent:       time.Now().UnixMilli(),
		ResponseAtAgent:      time.Now().UnixMilli(),
		ResponseAtHTTPServer: time.Now().UnixMilli(),
	}
	metadataBytes, _ := json.Marshal(m)

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(session.MetadataHeader, string(metadataBytes))
		w.WriteHeader(http.StatusOK)
	})
	handler := ing.front(inner)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest("GET", "/wapi/v2/resource", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
	}
}
