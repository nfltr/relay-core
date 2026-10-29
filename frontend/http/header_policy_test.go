// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/spf13/viper"
)

// ---------------------------------------------------------------------------
// Unit: HeaderPolicy.ApplyRequest
// ---------------------------------------------------------------------------

func TestHeaderPolicy_AddRequestHeader(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.request_headers.add", map[string]any{"X-Tenant": "acme"})
	p := NewHeaderPolicyFromConfig(testLogger())
	if p == nil {
		t.Fatal("expected non-nil policy")
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	p.ApplyRequest(req)
	if got := req.Header.Get("X-Tenant"); got != "acme" {
		t.Errorf("want X-Tenant=acme, got %q", got)
	}
}

func TestHeaderPolicy_RemoveRequestHeader(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.request_headers.remove", []string{"Authorization"})
	p := NewHeaderPolicyFromConfig(testLogger())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	p.ApplyRequest(req)
	if got := req.Header.Get("Authorization"); got != "" {
		t.Errorf("expected Authorization stripped, got %q", got)
	}
}

func TestHeaderPolicy_URLRewrite(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.url_rewrite.pattern", `^/api/v1/(.*)`)
	viper.Set("http.url_rewrite.replacement", "/v1/$1")
	p := NewHeaderPolicyFromConfig(testLogger())

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)
	p.ApplyRequest(req)
	if req.URL.Path != "/v1/users" {
		t.Errorf("want /v1/users, got %q", req.URL.Path)
	}
}

func TestHeaderPolicy_URLRewrite_NoMatch(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.url_rewrite.pattern", `^/api/v1/(.*)`)
	viper.Set("http.url_rewrite.replacement", "/v1/$1")
	p := NewHeaderPolicyFromConfig(testLogger())

	req := httptest.NewRequest(http.MethodGet, "/other/path", nil)
	p.ApplyRequest(req)
	if req.URL.Path != "/other/path" {
		t.Errorf("path should be unchanged, got %q", req.URL.Path)
	}
}

// ---------------------------------------------------------------------------
// Unit: HeaderPolicy.ApplyResponse
// ---------------------------------------------------------------------------

func TestHeaderPolicy_AddResponseHeader(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.response_headers.add", map[string]any{"X-Server": "rpc"})
	p := NewHeaderPolicyFromConfig(testLogger())

	h := http.Header{}
	p.ApplyResponse(h)
	if got := h.Get("X-Server"); got != "rpc" {
		t.Errorf("want X-Server=rpc, got %q", got)
	}
}

func TestHeaderPolicy_RemoveResponseHeader(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.response_headers.remove", []string{"Server"})
	p := NewHeaderPolicyFromConfig(testLogger())

	h := http.Header{"Server": []string{"nginx"}}
	p.ApplyResponse(h)
	if got := h.Get("Server"); got != "" {
		t.Errorf("expected Server stripped, got %q", got)
	}
}

// ---------------------------------------------------------------------------
// Unit: NilPolicy passthrough
// ---------------------------------------------------------------------------

func TestHeaderPolicy_Nil_IsNoop(t *testing.T) {
	defer viper.Reset()
	// No config set → NewHeaderPolicyFromConfig returns nil
	p := NewHeaderPolicyFromConfig(testLogger())
	if p != nil {
		t.Fatal("expected nil policy when no config is set")
	}
}

// ---------------------------------------------------------------------------
// Unit: InvalidPattern fallback
// ---------------------------------------------------------------------------

func TestHeaderPolicy_InvalidPattern_NoURLRewrite(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.url_rewrite.pattern", "[invalid(")
	viper.Set("http.request_headers.add", map[string]any{"X-Ok": "yes"})
	p := NewHeaderPolicyFromConfig(testLogger())
	// Policy should still be non-nil (request header add is valid)
	if p == nil {
		t.Fatal("expected non-nil policy")
	}
	if p.urlRewritePattern != nil {
		t.Error("invalid regex should result in nil urlRewritePattern")
	}
}

// ---------------------------------------------------------------------------
// Integration: HeaderRewriteMiddleware end-to-end
// ---------------------------------------------------------------------------

func TestHeaderRewriteMiddleware_NilPolicy(t *testing.T) {
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	mw := HeaderRewriteMiddleware(nil, next)
	// nil policy returns the original handler unchanged.
	_ = mw

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	mw.ServeHTTP(rw, req)
	if !called {
		t.Error("next handler was not called")
	}
}

func TestHeaderRewriteMiddleware_Applies(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.request_headers.add", map[string]any{"X-Injected": "yes"})
	viper.Set("http.response_headers.add", map[string]any{"X-Resp": "set"})
	p := NewHeaderPolicyFromConfig(testLogger())

	var gotInjected string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotInjected = r.Header.Get("X-Injected")
		w.WriteHeader(http.StatusOK)
	})
	mw := HeaderRewriteMiddleware(p, next)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	mw.ServeHTTP(rw, req)

	if gotInjected != "yes" {
		t.Errorf("request header injection: want yes, got %q", gotInjected)
	}
	if got := rw.Header().Get("X-Resp"); got != "set" {
		t.Errorf("response header injection: want set, got %q", got)
	}
}

func TestHeaderRewriteMiddleware_ResponseHeadersOnWrite(t *testing.T) {
	defer viper.Reset()
	viper.Set("http.response_headers.add", map[string]any{"X-Written": "1"})
	p := NewHeaderPolicyFromConfig(testLogger())

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Trigger via Write (no explicit WriteHeader call)
		_, _ = w.Write([]byte("hello"))
	})
	mw := HeaderRewriteMiddleware(p, next)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rw := httptest.NewRecorder()
	mw.ServeHTTP(rw, req)

	if got := rw.Header().Get("X-Written"); got != "1" {
		t.Errorf("want X-Written=1, got %q", got)
	}
}
