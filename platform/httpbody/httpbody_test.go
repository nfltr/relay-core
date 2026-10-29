// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpbody

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTooLargeMessageNamesTheLimit(t *testing.T) {
	for limit, want := range map[int64]string{1 << 30: "at most 1 GiB", 32 << 20: "at most 32 MiB", 1000: "at most 1000 bytes"} {
		if got := TooLargeMessage(limit); !strings.Contains(got, want) || !strings.Contains(got, "http.server.max_request_body_size") {
			t.Errorf("TooLargeMessage(%d) = %q, want %q and the setting", limit, got, want)
		}
	}
}

// A marked body cut before its length is the client's error; one read to
// its end, or an unmarked request, is not.
func TestIncompleteError(t *testing.T) {
	for _, tc := range []struct {
		r    *http.Request
		want bool
	}{
		{Mark(httptest.NewRequest("POST", "/", io.MultiReader(strings.NewReader("abc"), errReader{io.ErrUnexpectedEOF}))), true},
		{Mark(httptest.NewRequest("POST", "/", strings.NewReader("abc"))), false},
		{httptest.NewRequest("POST", "/", io.MultiReader(strings.NewReader("abc"), errReader{io.ErrUnexpectedEOF})), false},
	} {
		_, _ = io.ReadAll(tc.r.Body)
		if got := IncompleteError(tc.r); got != tc.want {
			t.Errorf("IncompleteError = %v, want %v", got, tc.want)
		}
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }
