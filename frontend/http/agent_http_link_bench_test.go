// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"io"
	"net/http"
	"net/http/httptest"
	"runtime/metrics"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
)

// BenchmarkAgentHTTPLinkRequest: one keep-alive request at a time through
// the relay's leaf (agentHTTP.ServeHTTP) to a generation-3 agent on an
// HTTP session and back: a GET of 1 KiB, and a POST of 1 KiB. allocs/op is
// the whole round in the process (relay, agent, backend);
// relay-agent-writes/op the relay's socket writes to the agent's
// connections per request; wakeups/op and goroutines/op the scheduler's
// (schedCounts); p50/p99 per request. bare/get-1k is the floor: the same
// GET straight to the backend, no relay and no agent.
// docs/performance.md.
func BenchmarkAgentHTTPLinkRequest(b *testing.B) {
	backend := startLinkHTTPBackend(b)
	h, leaf := startAgentHTTPLeaf(b, backend.URL, agentCurrent)
	body := strings.Repeat("x", 1024)
	bare := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	defer bare.CloseIdleConnections()
	for _, c := range []struct {
		name, method, path string
	}{
		{"bare/get-1k", http.MethodGet, "/dl?n=1024"},
		{"get-1k", http.MethodGet, "/dl?n=1024"},
		{"post-1k", http.MethodPost, "/echo"},
	} {
		b.Run(c.name, func(b *testing.B) {
			serve := func() {
				if strings.HasPrefix(c.name, "bare/") {
					resp, err := bare.Get(backend.URL + c.path)
					if err != nil {
						b.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					return
				}
				var r *http.Request
				if c.method == http.MethodPost {
					r = httptest.NewRequest(c.method, c.path, strings.NewReader(body))
				} else {
					r = httptest.NewRequest(c.method, c.path, nil)
				}
				r.Header.Set(session.AgentIDKey, harnessAgentID)
				w := httptest.NewRecorder()
				leaf.ServeHTTP(w, r)
				if w.Code != http.StatusOK {
					b.Fatalf("status %d: %s", w.Code, w.Body)
				}
			}
			serve() // the keep-alive connection
			w0 := h.writes.Load()
			var took []time.Duration
			b.ReportAllocs()
			s0, g0 := schedCounts()
			b.ResetTimer()
			for b.Loop() {
				start := time.Now()
				serve()
				took = append(took, time.Since(start))
			}
			b.StopTimer()
			s1, g1 := schedCounts()
			b.ReportMetric((s1-s0)/float64(b.N), "wakeups/op")
			b.ReportMetric((g1-g0)/float64(b.N), "goroutines/op")
			if !strings.HasPrefix(c.name, "bare/") {
				b.ReportMetric(float64(h.writes.Load()-w0)/float64(b.N), "relay-agent-writes/op")
			}
			slices.Sort(took)
			b.ReportMetric(float64(took[len(took)/2].Nanoseconds())/1e3, "p50-us")
			b.ReportMetric(float64(took[len(took)*99/100].Nanoseconds())/1e3, "p99-us")
		})
	}
}

// schedCounts reads the scheduler's run transitions (wakeups: the
// runtime samples 1 in 8 into /sched/latencies:seconds, so its count x8
// estimates them) and the goroutines created since the program started.
func schedCounts() (wakeups, created float64) {
	s := []metrics.Sample{{Name: "/sched/latencies:seconds"}, {Name: "/sched/goroutines-created:goroutines"}}
	metrics.Read(s)
	var n uint64
	for _, c := range s[0].Value.Float64Histogram().Counts {
		n += c
	}
	return float64(n) * 8, float64(s[1].Value.Uint64())
}
