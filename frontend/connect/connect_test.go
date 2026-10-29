// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestKey(t *testing.T) {
	for _, tc := range []struct{ name, header, value, key, from string }{
		{"none", "", "", "", ""},
		{"x-api-key", "X-API-Key", "k1", "k1", "X-API-Key"},
		{"bearer", "Authorization", "Bearer MyToken123", "MyToken123", "Authorization"},
		{"bearer any case", "Authorization", "BEARER abc", "abc", "Authorization"},
		{"just bearer", "Authorization", "Bearer ", "", ""},
		{"bearer no space", "Authorization", "Bearertoken", "", ""},
		{"apikey", "Authorization", "ApiKey k2", "k2", "Authorization"},
		{"basic", "Authorization", "Basic dXNlcjpwYXNz", "", ""},
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.header != "" {
			r.Header.Set(tc.header, tc.value)
		}
		if key, from := RequestKey(r); key != tc.key || from != tc.from {
			t.Errorf("%s: RequestKey = %q, %q; want %q, %q", tc.name, key, from, tc.key, tc.from)
		}
	}
}
