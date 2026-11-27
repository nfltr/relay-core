// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
)

// TestSharesRefusedAccount: a visit to a share whose agent's account the
// composition refuses (a prepaid account whose balance is exhausted) is
// answered 402 with reason ERROR_REASON_BALANCE_EXHAUSTED and never reaches
// the agent; any other account's share is served as before.
func TestSharesRefusedAccount(t *testing.T) {
	grants := grantsByCode{"broke": {AgentID: "bob.web"}, "open": {AgentID: "alice.web"}}
	s := &Shares{Grants: grants, Verified: NewShareVerified(), Log: slog.New(slog.DiscardHandler),
		Refuse: func(agentID string) error {
			if strings.HasPrefix(agentID, "bob.") {
				return identity.ErrBalanceExhausted
			}
			return nil
		}}
	reached := false
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { reached = true })
	w := httptest.NewRecorder()
	s.Serve(w, httptest.NewRequest(http.MethodGet, "/", nil), next, "broke")
	if w.Code != http.StatusPaymentRequired || reached || !strings.Contains(w.Body.String(), relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED.String()) ||
		!strings.Contains(w.Body.String(), "balance is exhausted") {
		t.Fatalf("a refused account's share: %d %q reached=%v, want 402 typed", w.Code, w.Body.String(), reached)
	}
	w = httptest.NewRecorder()
	s.Serve(w, httptest.NewRequest(http.MethodGet, "/", nil), next, "open")
	if !reached {
		t.Fatalf("another account's share: %d, not served", w.Code)
	}
}
