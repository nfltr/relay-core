// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"

	"github.com/nfltr/relay-core/internal/platform/expiring"
	"github.com/nfltr/relay-core/platform/httpbody"
)

func TestProxyCache_Get(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer ts.Close()

	target, _ := url.Parse(ts.URL)
	pc := &proxyCache{
		cache: expiring.New[string, *httputil.ReverseProxy](10, 0, nil),
	}
	proxy := pc.Get(target)
	if proxy == nil {
		t.Fatal("expected non-nil proxy")
	}
	// second call should return cached
	proxy2 := pc.Get(target)
	if proxy2 == nil {
		t.Fatal("expected non-nil cached proxy")
	}
}

// TestIncompleteBodyIsAClientError: a client whose body ends before its
// Content-Length gets 400, not 502, from the reverse-proxy hop (the relay's
// upload guard marks the body: httpbody.Mark).
func TestIncompleteBodyIsAClientError(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	t.Cleanup(backend.Close)
	pc := &proxyCache{transport: http.DefaultTransport, cache: expiring.New[string, *httputil.ReverseProxy](1, 0, nil)}
	u, _ := url.Parse(backend.URL)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { pc.Get(u).ServeHTTP(w, httpbody.Mark(r)) })
	r := httptest.NewRequest(http.MethodPost, "/upload", io.MultiReader(strings.NewReader("abc"), errReader{io.ErrUnexpectedEOF}))
	r.ContentLength = 1000
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "incomplete request body") {
		t.Fatalf("got %d %q, want 400 incomplete request body", w.Code, w.Body.String())
	}
}
