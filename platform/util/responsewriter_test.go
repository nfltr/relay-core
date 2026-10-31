// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeaderCleanupResponseWriter_WriteHeader(t *testing.T) {
	// Create a mock response writer
	mockRW := httptest.NewRecorder()

	// Create a custom headers slice
	customHeaders := []string{"Custom-Header-1", "Custom-Header-2"}

	// Create a HeaderCleanupResponseWriter instance
	rw := NewResponseWriter(mockRW, customHeaders)

	// Set some custom headers in the response writer
	mockRW.Header().Add("Custom-Header-1", "Value1")
	mockRW.Header().Add("Custom-Header-1", "Value2")
	mockRW.Header().Set("Custom-Header-2", "Value2")

	// Call WriteHeader with a status code
	rw.WriteHeader(http.StatusOK)

	// Check if the custom headers are removed from the response writer
	if len(mockRW.Header().Values("Custom-Header-1")) > 0 {
		t.Errorf("Custom-Header-1 should have been removed")
	}
	if len(mockRW.Header().Values("Custom-Header-2")) > 0 {
		t.Errorf("Custom-Header-2 should have been removed")
	}

	// Check if the removed headers are stored in the HeaderCleanupResponseWriter
	removedHeaders := rw.GetRemovedHeaders()
	if len(removedHeaders.Values("Custom-Header-1")) != 2 {
		t.Errorf("Custom-Header-1 should have been stored in removed headers")
	}
	if len(removedHeaders.Values("Custom-Header-2")) != 1 {
		t.Errorf("Custom-Header-2 should have been stored in removed headers")
	}
}

func TestHeaderCleanupResponseWriter_Write(t *testing.T) {
	customHeaders := []string{"Custom-Header-1", "Custom-Header-2"}

	firstMiddleware := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := NewResponseWriter(w, customHeaders)
			next.ServeHTTP(rw, r)
			removedHeaders := rw.GetRemovedHeaders()
			if len(removedHeaders.Values("Custom-Header-1")) != 2 {
				t.Errorf("Custom-Header-1 should have been stored in removed headers")
			}
			if len(removedHeaders.Values("Custom-Header-2")) != 1 {
				t.Errorf("Custom-Header-2 should have been stored in removed headers")
			}
		})
	}

	handlerChain := firstMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range r.Header {
			for _, x := range v {
				w.Header().Add(k, x)
			}
		}
		_, _ = w.Write([]byte("Hello, World!"))
	}))

	srv := httptest.NewServer(handlerChain)
	defer srv.Close()

	r, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	r.Header.Add("Custom-Header-1", "Value1")
	r.Header.Add("Custom-Header-1", "Value2")
	r.Header.Set("Custom-Header-2", "Value2")

	resp, err := srv.Client().Do(r)
	if err != nil {
		t.Errorf("Error making HTTP request: %v", err)
		return
	}
	defer resp.Body.Close() //nolint:errcheck

	if len(resp.Header.Values("Custom-Header-1")) > 0 {
		t.Errorf("Custom-Header-1 should have been removed")
	}

	if len(resp.Header.Values("Custom-Header-2")) > 0 {
		t.Errorf("Custom-Header-2 should have been removed")
	}
}

// A 1xx (103 Early Hints) is passed through and is not the final status:
// the final WriteHeader still applies and still removes the headers.
// Before, the 1xx counted as final: a later 404 went out as an implicit
// 200, with the headers to remove.
func TestFinalStatusAfter1xx(t *testing.T) {
	var got int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := NewResponseWriter(w, []string{"X-Internal"})
		rw.WriteHeader(http.StatusEarlyHints)
		rw.Header().Set("X-Internal", "secret")
		rw.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(rw, "nope")
		got = rw.GetStatusCode()
	}))
	defer srv.Close()
	res, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotFound || res.Header.Get("X-Internal") != "" || got != http.StatusNotFound {
		t.Fatalf("status %d (recorded %d), X-Internal %q; want 404 and the header removed", res.StatusCode, got, res.Header.Get("X-Internal"))
	}
}
