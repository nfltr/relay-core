// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nfltr/relay-core/core/session"
)

// TestFrontDropsMetadataWithoutWriteHeader: front takes the agent metadata
// header off the response also when the handler behind it writes no status
// (net/http then sends the implicit 200 with the header map as it is), and
// counts that response as the 200 it is.
func TestFrontDropsMetadataWithoutWriteHeader(t *testing.T) {
	ing := newTestIngress(&session.Core{})
	before := requestsTotal(t, "PATCH", http.StatusOK)
	w := httptest.NewRecorder()
	ing.front(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(session.MetadataHeader, `{"a":"agent"}`)
	})).ServeHTTP(w, httptest.NewRequest(http.MethodPatch, "/", nil))
	if got := w.Result().Header.Get(session.MetadataHeader); got != "" {
		t.Errorf("the response carried the agent metadata header %q", got)
	}
	if got := requestsTotal(t, "PATCH", http.StatusOK) - before; got != 1 {
		t.Errorf("requests_total{status=200} grew by %v, want 1", got)
	}
}
