// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/core/sharesecret"
	"github.com/nfltr/relay-core/platform/ratelimit"
	"github.com/nfltr/relay/proto/wire/agentservices"
)

type grantsByCode map[string]frontend.Grant

func (g grantsByCode) IsCode(s string) bool { return s != "" }
func (g grantsByCode) Grant(code string) (frontend.Grant, error) {
	if code == "down" {
		return frontend.Grant{}, errors.New("store unreachable")
	}
	if gr, ok := g[code]; ok {
		return gr, nil
	}
	return frontend.Grant{}, frontend.ErrNoGrant
}

// TestSharesServe: a share's visitor reaches its agent (or service) with
// the agent named and its X-Robots-Tag; its policy's credential is checked
// and removed; an unknown code is 401 and a failed lookup or credential
// counts against the client's budget, after which it is 429 before any
// lookup; an unanswering verifier is 503 and costs nothing; Direct names
// an agent a label grants nothing for.
func TestSharesServe(t *testing.T) {
	grants := grantsByCode{
		"open":  {AgentID: "alice.web"},
		"svc":   {AgentID: "alice.web", Service: "db"},
		"gated": {AgentID: "alice.web", Policy: frontend.SharePolicy{BasicAuth: sharesecret.Seal("v:pw")}},
	}
	s := &Shares{
		Grants: grants, Verified: NewShareVerified(), Misses: ratelimit.New(1, 2, time.Minute, time.Minute), Log: slog.New(slog.DiscardHandler),
		Direct:    func(_ *http.Request, label string) (string, bool) { return "alice." + label, label == "web" },
		RobotsTag: "noindex",
	}
	var reached *http.Request
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { reached = r })
	visit := func(code, ip string, basic ...string) *httptest.ResponseRecorder {
		reached = nil
		r := httptest.NewRequest(http.MethodGet, "/p", nil)
		r.RemoteAddr = ip + ":1234"
		r.Header.Set(agentservices.RequestHeader, "forged")
		if len(basic) == 2 {
			r.SetBasicAuth(basic[0], basic[1])
		}
		w := httptest.NewRecorder()
		s.Serve(w, r, next, code)
		return w
	}

	if w := visit("open", "192.0.2.1"); reached == nil || reached.Header.Get(session.AgentIDKey) != "alice.web" ||
		reached.Header.Get(agentservices.RequestHeader) != "" || w.Header().Get("X-Robots-Tag") != "noindex" {
		t.Fatalf("an open share: %d, reached %v", w.Code, reached)
	}
	if visit("svc", "192.0.2.1"); reached == nil || reached.Header.Get(agentservices.RequestHeader) != "db" {
		t.Fatalf("a service's share did not name its service: %v", reached)
	}
	if w := visit("gated", "192.0.2.1", "v", "pw"); reached == nil || reached.Header.Get("Authorization") != "" {
		t.Fatalf("a Basic Auth share with its credential: %d, reached %v", w.Code, reached)
	}
	if w := visit("web", "192.0.2.1"); reached == nil || reached.Header.Get(session.AgentIDKey) != "alice.web" {
		t.Fatalf("Direct: %d, reached %v", w.Code, reached)
	}
	if w := visit("down", "192.0.2.2"); w.Code != http.StatusServiceUnavailable || reached != nil {
		t.Fatalf("an unanswering verifier: %d", w.Code)
	}

	// Misses: a wrong credential and an unknown code, then the budget is spent.
	if w := visit("gated", "192.0.2.2", "v", "wrong"); w.Code != http.StatusUnauthorized || reached != nil {
		t.Fatalf("a wrong credential: %d", w.Code)
	}
	if w := visit("nope", "192.0.2.2"); w.Code != http.StatusUnauthorized || reached != nil {
		t.Fatalf("an unknown code: %d", w.Code)
	}
	if w := visit("open", "192.0.2.2"); w.Code != http.StatusTooManyRequests || reached != nil {
		t.Fatalf("over its budget, a valid share: %d, want 429", w.Code)
	}
	if visit("open", "192.0.2.3"); reached == nil {
		t.Fatal("another client's budget was spent")
	}
}

// TestSharesRealmAndRobotsTag: a Basic Auth share's challenge names Realm
// (frontend.DefaultRealm when unset), and a visitor's response carries
// RobotsTag only when set.
func TestSharesRealmAndRobotsTag(t *testing.T) {
	grants := grantsByCode{
		"open":  {AgentID: "alice.web"},
		"gated": {AgentID: "alice.web", Policy: frontend.SharePolicy{BasicAuth: sharesecret.Seal("v:pw")}},
	}
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	for _, tc := range []struct{ realm, robots, wantChallenge string }{
		{"", "", `Basic realm="relay"`},
		{"example", "noindex, nofollow", `Basic realm="example"`},
	} {
		s := &Shares{Grants: grants, Verified: NewShareVerified(), Log: slog.New(slog.DiscardHandler), Realm: tc.realm, RobotsTag: tc.robots}
		w := httptest.NewRecorder()
		s.Serve(w, httptest.NewRequest(http.MethodGet, "/", nil), next, "gated")
		if got := w.Header().Get("WWW-Authenticate"); w.Code != http.StatusUnauthorized || got != tc.wantChallenge {
			t.Errorf("realm %q: %d %q, want 401 %q", tc.realm, w.Code, got, tc.wantChallenge)
		}
		w = httptest.NewRecorder()
		s.Serve(w, httptest.NewRequest(http.MethodGet, "/", nil), next, "open")
		if got := w.Header().Get("X-Robots-Tag"); got != tc.robots {
			t.Errorf("X-Robots-Tag %q, want %q", got, tc.robots)
		}
	}
}
