// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nfltr/relay-core/core/frontend"
)

// TestSharesSignIn: a share whose rule is sign-in reaches its agent only
// when the composition's SignIn admits the request; without a SignIn it
// is 503 (fail closed, never open); a share without sign-in never asks.
func TestSharesSignIn(t *testing.T) {
	grants := grantsByCode{
		"open":  {AgentID: "alice.web"},
		"login": {AgentID: "alice.web", Policy: frontend.SharePolicy{Login: true}},
	}
	var reached bool
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true })
	visit := func(s *Shares, code string) *httptest.ResponseRecorder {
		reached = false
		w := httptest.NewRecorder()
		s.Serve(w, httptest.NewRequest(http.MethodGet, "/p", nil), next, code)
		return w
	}

	none := &Shares{Grants: grants, Verified: NewShareVerified(), Log: slog.New(slog.DiscardHandler)}
	if w := visit(none, "login"); w.Code != http.StatusServiceUnavailable || reached {
		t.Fatalf("a sign-in share without SignIn: %d, reached %v; want 503, not reached", w.Code, reached)
	}

	var asked []string
	admit := false
	s := &Shares{Grants: grants, Verified: NewShareVerified(), Log: slog.New(slog.DiscardHandler),
		SignIn: func(w http.ResponseWriter, _ *http.Request, code string, g frontend.Grant) bool {
			asked = append(asked, code+"/"+g.AgentID)
			if !admit {
				w.WriteHeader(http.StatusSeeOther)
			}
			return admit
		}}
	if w := visit(s, "login"); w.Code != http.StatusSeeOther || reached {
		t.Fatalf("a sign-in share SignIn refused: %d, reached %v", w.Code, reached)
	}
	admit = true
	if visit(s, "login"); !reached {
		t.Fatal("a sign-in share SignIn admitted: not reached")
	}
	if visit(s, "open"); !reached || len(asked) != 2 {
		t.Fatalf("an open share: reached %v, SignIn asked %v (want only the sign-in share's two)", reached, asked)
	}
}
