// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"testing"

	"github.com/nfltr/link"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestWSCloseCode: a browser's session ends with the close code of the
// CLI's exit for the same refusal (nf1.Dial's answerErr): not served 4003,
// balance 4009, any other reason 4004 with it in the reason, link's own
// refusals (draining, no route) 1013, an abort after the open 1011.
func TestWSCloseCode(t *testing.T) {
	r := dataplane.Refusal
	for _, tc := range []struct {
		a    *link.Abort
		code uint16
	}{
		{r(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, "x"), 4003},
		{r(relayv1.ErrorReason_ERROR_REASON_AGENT_NOT_FOUND, "x"), 4003},
		{r(relayv1.ErrorReason_ERROR_REASON_UNKNOWN_SERVICE, "x"), 4003},
		{r(relayv1.ErrorReason_ERROR_REASON_BALANCE_EXHAUSTED, "x"), 4009},
		{r(relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "x"), 4004},
		{r(relayv1.ErrorReason_ERROR_REASON_RELAY_DRAINING, "x"), 4004},
		{&link.Abort{Refused: true, Code: link.CodeDraining}, 1013},
		{&link.Abort{Refused: true, Code: link.CodeNoRoute}, 1013},
		{&link.Abort{Code: dataplane.AbortCode(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE)}, 1011},
	} {
		if code, _ := wsCloseCode(tc.a); code != tc.code {
			t.Errorf("%+v: %d, want %d", tc.a, code, tc.code)
		}
	}
	if _, reason := wsCloseCode(r(relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "over")); reason != "ERROR_REASON_LIMIT_EXCEEDED: over" {
		t.Errorf("reason %q", reason)
	}
}
