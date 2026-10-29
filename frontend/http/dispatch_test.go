// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/spf13/viper"
)

func TestRewriteLocationHeader_AbsolutePath(t *testing.T) {
	h := http.Header{}
	h.Set("Location", "/page2")
	rewriteLocationHeader(h, "/browse/my-agent")

	want := "/browse/my-agent/page2"
	if got := h.Get("Location"); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestRewriteLocationHeader_AlreadyPrefixed(t *testing.T) {
	h := http.Header{}
	h.Set("Location", "/browse/my-agent/page2")
	rewriteLocationHeader(h, "/browse/my-agent")

	want := "/browse/my-agent/page2"
	if got := h.Get("Location"); got != want {
		t.Errorf("got %q, want %q (should not double-prefix)", got, want)
	}
}

func TestRewriteLocationHeader_FullURL(t *testing.T) {
	h := http.Header{}
	h.Set("Location", "http://example.com/page")
	rewriteLocationHeader(h, "/browse/my-agent")

	want := "http://example.com/page"
	if got := h.Get("Location"); got != want {
		t.Errorf("got %q, want %q (should not rewrite full URLs)", got, want)
	}
}

func TestRewriteLocationHeader_Relative(t *testing.T) {
	h := http.Header{}
	h.Set("Location", "next-page")
	rewriteLocationHeader(h, "/browse/my-agent")

	want := "next-page"
	if got := h.Get("Location"); got != want {
		t.Errorf("got %q, want %q (should not rewrite relative)", got, want)
	}
}

func TestRewriteLocationHeader_Empty(t *testing.T) {
	h := http.Header{}
	rewriteLocationHeader(h, "/browse/my-agent")

	if got := h.Get("Location"); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestRewriteLocationHeader_Root(t *testing.T) {
	h := http.Header{}
	h.Set("Location", "/")
	rewriteLocationHeader(h, "/browse/agent-1")

	want := "/browse/agent-1/"
	if got := h.Get("Location"); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestBrowseResponseWriter_RewritesLocation(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := &browseResponseWriter{
		ResponseWriter: rec,
		browsePrefix:   "/browse/my-agent",
	}

	bw.Header().Set("Location", "/fossil/home")
	bw.WriteHeader(http.StatusFound)

	got := rec.Header().Get("Location")
	want := "/browse/my-agent/fossil/home"
	if got != want {
		t.Fatalf("Location: got %q, want %q", got, want)
	}
	if rec.Code != http.StatusFound {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusFound)
	}
}

func TestBrowseResponseWriter_NoLocation(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := &browseResponseWriter{
		ResponseWriter: rec,
		browsePrefix:   "/browse/my-agent",
	}

	bw.WriteHeader(http.StatusOK)

	if loc := rec.Header().Get("Location"); loc != "" {
		t.Fatalf("Location should be empty, got %q", loc)
	}
}

func TestBrowseResponseWriter_FullURL_NotRewritten(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := &browseResponseWriter{
		ResponseWriter: rec,
		browsePrefix:   "/browse/my-agent",
	}

	bw.Header().Set("Location", "http://example.com/page")
	bw.WriteHeader(http.StatusFound)

	got := rec.Header().Get("Location")
	if got != "http://example.com/page" {
		t.Fatalf("full URL should not be rewritten, got %q", got)
	}
}

func TestBrowseResponseWriter_Flush(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := &browseResponseWriter{
		ResponseWriter: rec,
		browsePrefix:   "/browse/my-agent",
	}
	// Should not panic even if underlying writer supports Flusher
	bw.Flush()
}

func TestDispatchRequestMiddleware_MissingAgentID(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: directory.NewNoOpInventory()})
	// Need a dispatcher for dispatchRequestMiddleware
	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	handler := http.HandlerFunc(ing.dispatch)
	req := httptest.NewRequest("GET", "/wapi/v2.13.4/resource", nil)
	// No AgentID header
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestDispatchRequestMiddleware_AgentNotRegistered(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	inv := &errorInventory{err: fmt.Errorf("not found")}
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})
	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, inv, inv, 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	handler := http.HandlerFunc(ing.dispatch)
	req := httptest.NewRequest("GET", "/wapi/v2.13.4/resource", nil)
	req.Header.Set(session.AgentIDKey, "nonexistent-agentID")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	// Agent is not registered at all → 404 (unknown agent)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestDispatchRequestMiddleware_RegisteredAgentNotConnected(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	inv := &errorInventory{err: fmt.Errorf("not found")}
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})
	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, inv, inv, 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d
	ing.core.PodID = "relay-0"

	// Pre-register the agent so it is "known" but not connected
	ing.registered = func(_ context.Context, id string) bool { return id == "registered-agentID" }

	handler := http.HandlerFunc(ing.dispatch)
	req := httptest.NewRequest("GET", "/wapi/v2.13.4/resource", nil)
	req.Header.Set(session.AgentIDKey, "registered-agentID")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	// Registered but not connected → 503 + Retry-After header
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
	if w.Header().Get("Retry-After") == "" {
		t.Error("expected Retry-After header on 503")
	}
	// Share visitors read the body: no relay internals (pods, agent ids).
	if body := strings.TrimSpace(w.Body.String()); body != agentOfflineMessage {
		t.Errorf("503 body = %q, want %q", body, agentOfflineMessage)
	}
	if got := w.Header().Get("X-Relay-Pod"); got != "relay-0" {
		t.Errorf("X-Relay-Pod = %q, want the pod for operators", got)
	}
}

func TestGetDispatcherURL_AgentConnected(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: directory.NewNoOpInventory()})

	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	// Store an agent in the dispatcher
	mock := &mockHTTPStreamer{}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	d.Agents.Store("agentID-1", session.NewHTTPStream("agentID-1", mock, nil, nil, ticker))

	dispatcherURL, err := ing.dispatcherURL(context.Background(), "agentID-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dispatcherURL == nil {
		t.Fatal("expected non-nil URL")
	}
	if dispatcherURL != ing.loopbackURL {
		t.Fatalf("expected the in-process sentinel, got %s", dispatcherURL.String())
	}
}

func TestGetDispatcherURL_AgentNotConnected(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	inv := &errorInventory{err: fmt.Errorf("not found")}
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})

	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, inv, inv, 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	_, err := ing.dispatcherURL(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for non-connected agent")
	}
}

func TestGetDispatcherURL_SameEnv(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("peer_link.port", 7443)
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Setenv("TEST_POD_IP", "10.0.0.99") // different from cache value so no replacement
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	mc := newMockCache()
	mc.data["dipi:agentID:agentID-1"] = &directory.Value{PodIP: "10.0.0.5"}

	inv, _ := directory.NewIPInventory(testLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})

	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, inv, inv, 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	dispatcherURL, err := ing.dispatcherURL(context.Background(), "agentID-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dispatcherURL == nil {
		t.Fatal("expected URL")
	}
	// Same env: the peer's pod IP and its replica link port (the link
	// dialer carries the request; no dispatcher port is listened on).
	expected := "http://10.0.0.5:7443"
	if dispatcherURL.String() != expected {
		t.Fatalf("expected %s, got %s", expected, dispatcherURL.String())
	}
}

func TestRPCServer_getDispatcherURL_SameEnv(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	viper.Set("peer_link.port", 7443)
	t.Setenv("TEST_POD_IP", "10.0.0.99")
	defer viper.Reset()

	mc := newMockCache()
	inv, _ := directory.NewIPInventory(testLogger(), mc, 5*time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})

	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	// Set a remote agent IP in cache
	_ = inv.SetIP(ctx, "agentID-remote", time.Minute)
	viper.Reset()

	dispatcherURL, err := ing.dispatcherURL(ctx, "agentID-remote")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Same-env agents route to that pod at its replica link port.
	if !strings.Contains(dispatcherURL.String(), "10.0.0.99:7443") {
		t.Fatalf("expected same-env pod dispatcher URL, got %s", dispatcherURL.String())
	}
}

// TestDispatchRequestMiddleware_AgentConnected_ServedInProcess: a request
// for an agent connected to this relay goes to the dispatcher's handler in
// process, never through an HTTP hop to the relay's own dispatcher port.
func TestDispatchRequestMiddleware_AgentConnected_ServedInProcess(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("loopback", "127.0.0.1")
	defer viper.Reset()

	// The replica link port: any request reaching it is a loopback hop.
	var hops atomic.Int32
	loopback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops.Add(1)
		_, _ = w.Write([]byte("dispatched over loopback"))
	}))
	defer loopback.Close()
	viper.Set("peer_link.port", strings.Split(loopback.URL, ":")[2])

	b := broker.NewInMemoryMessageBroker()
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: directory.NewNoOpInventory()})

	ctx := context.Background()
	d := session.NewDispatcher(ctx, testLogger(), b, &testProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, session.DispatcherConfig{
		ConnectedAgentsThreshold: 100,
	})
	ing.core.Dispatcher = d

	// Store an agent so it appears connected
	mock := &mockHTTPStreamer{}
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	d.Agents.Store("agentID-forward", session.NewHTTPStream("agentID-forward", mock, nil, nil, ticker))

	handler := http.HandlerFunc(ing.dispatch)
	req := httptest.NewRequest("GET", "/wapi/v2.13.4/resource", nil)
	req.Header.Set(session.AgentIDKey, "agentID-forward")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if n := hops.Load(); n != 0 {
		t.Fatalf("%d request(s) went over the loopback hop to the dispatcher port", n)
	}
	// The dispatcher's own handler answered: it refuses a request the
	// relay's first middleware did not stamp (no request-at header).
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want the in-process dispatcher's 400; body: %s", w.Code, w.Body.String())
	}
}
