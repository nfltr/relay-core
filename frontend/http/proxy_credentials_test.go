// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/wire/chunking"
)

// A shared or browsed agent must not receive the visitor's relay session or
// X-API-Key; its own cookies still pass through, and so does Authorization:
// the relay removes it before here when it authenticated the request with
// it (withoutRelayAuthorization, frontend.SharePolicy.Enforce),
// and leaves a header meant for the service alone.
func TestPrepareAgentRequestDropsRelayCredentials(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Cookie", "app=1; rpc_session=secret-session; rpc_browse=b; __Host-rpc_session=s2; __Host-rpc_browse=b2")
	r.Header.Add("Cookie", "oauth_state=s; __Host-oauth_pkce=p; __Host-cli_signup=c")
	r.Header.Set("X-API-Key", "secret-key")
	r.Header.Set("Authorization", "Bearer the-apps-own")
	req, body, err := prepareRequest(r, "agent", "txn", time.Now().Add(time.Minute), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close() //nolint:errcheck
	got := req.Request.Headers
	if c := got["Cookie"].GetHeaderValues(); len(c) != 1 || c[0] != "app=1" {
		t.Fatalf("Cookie forwarded = %v, want only the agent's own cookie", c)
	}
	if v, ok := got["X-Api-Key"]; ok {
		t.Fatalf("X-Api-Key forwarded to agent: %v", v.GetHeaderValues())
	}
	if a := got["Authorization"].GetHeaderValues(); len(a) != 1 || a[0] != "Bearer the-apps-own" {
		t.Fatalf("Authorization forwarded = %v, want the app's own token", a)
	}
	if len(r.Header.Values("Cookie")) != 2 {
		t.Fatal("PrepareAgentRequest mutated the caller's request headers")
	}
}

// A client cannot forge a flow-control frame through its own request headers.
func TestPrepareAgentRequestDropsClientFlowControlHeaders(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/f", nil)
	r.Header.Set(chunking.HeaderCancel, "1")
	r.Header.Set(chunking.HeaderAck, "3")
	r.Header.Set(chunking.HeaderUploadAcks, "1")
	req, _, err := prepareRequest(r, "a", "txn", time.Now(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if chunking.IsControl(req.Request.Headers) || chunking.WantsUploadAcks(req.Request.Headers) {
		t.Fatalf("client headers reached the agent as a flow-control frame: %v", req.Request.Headers)
	}
}
