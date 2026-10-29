// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/spf13/viper"
)

func TestFirstMiddleware_Success(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b})

	// Create a response with proper metadata
	m := Metadata{
		AgentID:             "agentID-1",
		RequestAtHTTPServer: time.Now().UnixMilli(),
		RequestAtAgent:      time.Now().UnixMilli(),
		ResponseAtAgent:     time.Now().UnixMilli(),
	}
	metadataBytes, _ := json.Marshal(m)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate a real response like the proxy would produce
		w.Header().Set(session.MetadataHeader, string(metadataBytes))
		w.WriteHeader(200)
	})

	handler := ing.front(inner)
	req := httptest.NewRequest("GET", "/wapi/v2.13.4/resource", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestFirstMiddleware_BadMetadata(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b})

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(session.MetadataHeader, "not-valid-json")
		w.WriteHeader(200)
	})

	handler := ing.front(inner)
	req := httptest.NewRequest("GET", "/wapi/v2.13.4/resource", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	// Should still return successfully even with bad metadata parse
}

func TestFirstMiddleware_Timeout(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b})

	m := Metadata{AgentID: "agentID-1"}
	metadataBytes, _ := json.Marshal(m)

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(session.MetadataHeader, string(metadataBytes))
		w.WriteHeader(http.StatusGatewayTimeout)
	})

	handler := ing.front(inner)
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
}

func TestGetAPIType(t *testing.T) {
	viper.Set("http.api_type_prefixes", []string{"wapi", "ibap"})
	defer viper.Reset()

	type args struct {
		r *http.Request
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "Testing wapi",
			args: args{
				r: &http.Request{
					URL: &url.URL{Path: "/wapi/users"},
				},
			},
			want: "wapi",
		},
		{
			name: "Testing ibap",
			args: args{
				r: &http.Request{
					URL: &url.URL{Path: "/ibap/users"},
				},
			},
			want: "ibap",
		},
		{
			name: "Testing negative testcase for wapi",
			args: args{
				r: &http.Request{
					URL: &url.URL{Path: "/wapier/users"},
				},
			},
			want: "other",
		},
		{
			name: "Testing negative testcase for ibap",
			args: args{
				r: &http.Request{
					URL: &url.URL{Path: "/ibapier/users"},
				},
			},
			want: "other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetAPIType(tt.args.r); got != tt.want {
				t.Errorf("GetAPIType() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSiteHostKeepsAgentCaching: every agent response is no-store, except a
// site host's, whose static pages keep the agent's own Cache-Control.
func TestSiteHostKeepsAgentCaching(t *testing.T) {
	h := newTestIngress(&session.Core{}).front(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Cache-Control", "no-cache")
	}))
	for site, want := range map[bool]string{false: "no-store,no-cache", true: "no-cache"} {
		req := httptest.NewRequest(http.MethodGet, "https://site.test/", nil)
		if site {
			req = req.WithContext(frontend.WithSiteRequest(req.Context()))
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if got := strings.Join(rec.Header().Values("Cache-Control"), ","); got != want {
			t.Errorf("site=%v: Cache-Control %q, want %q", site, got, want)
		}
	}
}

// TestAPIType: the first path segment, as GetAPIType's split took it.
func TestAPIType(t *testing.T) {
	prefixes := []string{"wapi", "ibap"}
	for path, want := range map[string]string{
		"/wapi/users": "wapi", "/ibap": "ibap", "/ibap/": "ibap", "/wapier/x": "other",
		"/": "other", "": "other", "wapi": "other", "x/wapi/y": "wapi", "//wapi": "other",
	} {
		if got := apiType(path, prefixes); got != want {
			t.Errorf("apiType(%q) = %q, want %q", path, got, want)
		}
	}
}

// TestFrontAllocBudget is the guard of R2, R4 and R5 (docs/performance.md):
// what front adds to a request (its transaction ID, the response writer,
// the metadata, metrics), counted against the same request served without
// it. A budget may only go down, but for new work front must do: the
// forwarded headers (setForwardedHeaders) cost one, their three values'
// backing array.
func TestFrontAllocBudget(t *testing.T) {
	const budget = 18 // 78 allocs/op in BenchmarkFirstMiddleware before R2, 35 after R5; 17 before the forwarded headers
	ing := newTestIngress(&session.Core{})
	metadata, _ := json.Marshal(Metadata{AgentID: "a", RequestAtAgent: 1, ResponseAtAgent: 2})
	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(session.MetadataHeader, string(metadata))
		w.WriteHeader(http.StatusOK)
	})
	serve := func(h http.Handler) func() {
		return func() {
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/wapi/v2/resource", nil))
		}
	}
	serve(ing.front(inner))() // metrics' label sets
	front := leastAllocsPerRun(200, serve(ing.front(inner))) - leastAllocsPerRun(200, serve(inner))
	t.Logf("front: %.2f allocations per request", front)
	if front > budget+raceAllocs+0.5 {
		t.Fatalf("front: %.2f allocations per request, budget %d", front, budget+raceAllocs)
	}
}

// leastAllocsPerRun is testing.AllocsPerRun's count, unrounded, in the
// least of five rounds. Mallocs is the process's: another goroutine's
// allocations (a goroutine an earlier test left, which a loaded machine
// schedules inside the round) only add to a round, and a mean rounded down
// on each side of a difference moved it by one at a boundary (the race
// detector drops a quarter of sync.Pool puts at random). The Linux runner
// once read 21 where every local run reads 18-19.
func leastAllocsPerRun(runs int, f func()) float64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	f()
	var ms runtime.MemStats
	least := float64(-1)
	for range 5 {
		runtime.ReadMemStats(&ms)
		before := ms.Mallocs
		for range runs {
			f()
		}
		runtime.ReadMemStats(&ms)
		if n := float64(ms.Mallocs-before) / float64(runs); least < 0 || n < least {
			least = n
		}
	}
	return least
}

// TestLeastAllocsPerRunIgnoresANoisyRound: allocations in one round only
// (another goroutine's, inside the measurement) are not the function's.
func TestLeastAllocsPerRunIgnoresANoisyRound(t *testing.T) {
	var sink []byte
	calls := 0
	f := func() {
		if calls++; calls <= 201 { // the warm-up call and the first round
			sink = make([]byte, 64)
		}
	}
	if n := leastAllocsPerRun(200, f); n != 0 {
		t.Fatalf("%v allocations per run, want 0", n)
	}
	_ = sink
}
