// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"reflect"
	"testing"
)

// TestStripHopByHopHeadersMatchesReverseProxy: an agent connected to this
// relay is served in process, and must see the headers it saw when the
// request crossed httputil.ReverseProxy (a peer's forward over the replica link).
func TestStripHopByHopHeadersMatchesReverseProxy(t *testing.T) {
	cases := map[string]http.Header{
		"plain": {"Accept": {"*/*"}, "X-Custom": {"a", "b"}},
		"hop-by-hop": {
			"Connection": {"keep-alive, X-Drop-Me"}, "X-Drop-Me": {"1"}, "Keep-Alive": {"timeout=5"},
			"Proxy-Authorization": {"Basic x"}, "Proxy-Connection": {"keep-alive"}, "Te": {"gzip"},
			"Trailer": {"X-T"}, "Accept": {"*/*"},
		},
		"upgrade":  {"Connection": {"Upgrade"}, "Upgrade": {"websocket"}, "Sec-Websocket-Key": {"k"}},
		"trailers": {"Te": {"trailers"}, "Accept": {"*/*"}},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			var viaProxy http.Header
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				viaProxy = r.Header.Clone()
			}))
			defer backend.Close()
			u, _ := url.Parse(backend.URL)
			front := httptest.NewServer(httputil.NewSingleHostReverseProxy(u))
			defer front.Close()

			req, _ := http.NewRequest(http.MethodGet, front.URL, nil)
			req.Header = h.Clone()
			if resp, err := http.DefaultTransport.RoundTrip(req); err == nil {
				_ = resp.Body.Close()
			}

			got := h.Clone()
			stripHopByHopHeaders(got)
			// Added by the proxy, the transport or the server, not by the rewrite.
			for _, k := range []string{"X-Forwarded-For", "Accept-Encoding", "User-Agent"} {
				viaProxy.Del(k)
				got.Del(k)
			}
			if !reflect.DeepEqual(got, viaProxy) {
				t.Errorf("in process %v, over the reverse proxy %v", got, viaProxy)
			}
		})
	}
}
