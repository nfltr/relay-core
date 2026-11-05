// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestBalanceExhaustedTypedOnConnect: a caller whose account's balance is
// exhausted is answered 402 with reason ERROR_REASON_BALANCE_EXHAUSTED on
// a data connection (never the 401 of a bad key), and its open sessions
// end with that reason when the account is cut.
func TestBalanceExhaustedTypedOnConnect(t *testing.T) {
	f := &Frontend{
		Config: func() Config { return Config{Enabled: true} },
		Caller: func(*http.Request) (identity.Principal, error) {
			return identity.Principal{}, identity.ErrBalanceExhausted
		},
	}
	r := httptest.NewRequest(http.MethodGet, dataplane.UpgradePath, nil)
	r.Header.Set(agentwire.GenerationKey, "2")
	w := httptest.NewRecorder()
	f.ServeHTTP(w, r)
	if w.Code != http.StatusPaymentRequired || !strings.Contains(w.Body.String(), relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED.String()) {
		t.Fatalf("answered %d %q, want 402 with reason ERROR_REASON_BALANCE_EXHAUSTED", w.Code, w.Body.String())
	}
	if got := Status(relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED); got != http.StatusPaymentRequired {
		t.Fatalf("Status = %d, want 402", got)
	}
	a := credentialAbort(identity.ErrBalanceExhausted)
	if reason, ok := dataplane.AbortReason(a); !ok || reason != relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED {
		t.Fatalf("the cut's abort = %v, want reason ERROR_REASON_BALANCE_EXHAUSTED", a)
	}
}
