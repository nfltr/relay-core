// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequestToken(t *testing.T) {
	for _, tc := range []struct{ name, header, value, key, from string }{
		{"none", "", "", "", ""},
		{"x-api-key is no credential", "X-API-Key", "k1", "", ""},
		{"bearer", "Authorization", "Bearer MyToken123", "MyToken123", "Authorization"},
		{"bearer any case", "Authorization", "BEARER abc", "abc", "Authorization"},
		{"just bearer", "Authorization", "Bearer ", "", ""},
		{"bearer no space", "Authorization", "Bearertoken", "", ""},
		{"apikey scheme is no credential", "Authorization", "ApiKey k2", "", ""},
		{"basic", "Authorization", "Basic dXNlcjpwYXNz", "", ""},
	} {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.header != "" {
			r.Header.Set(tc.header, tc.value)
		}
		if key, from := requestToken(r); key != tc.key || from != tc.from {
			t.Errorf("%s: requestToken = %q, %q; want %q, %q", tc.name, key, from, tc.key, tc.from)
		}
	}
}
