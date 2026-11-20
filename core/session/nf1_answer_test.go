// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"testing"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/link/peerlink/quiclink"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// answerLeg is a native leg whose answer is err.
type answerLeg struct {
	*link.Session
	err error
}

func (l answerLeg) Answered(context.Context) error { return l.err }
func (answerLeg) Answers() bool                    { return true }

// An nf1 open's agent gone before it answered (its leg's link refusal)
// answers not served, as an unknown address does; the agent's own
// refusal passes on (spec/NF1Claim.tla Answer, OneAnswer, mutant
// lost-answer; lane PIPERESET2: TestNF1CapsAndNotServed read "relay
// unreachable"). The leg keeps every end method the joint uses.
func TestNF1LegAnswersNotServedWhenTheAgentLeft(t *testing.T) {
	var _ nativeLeg = (*link.Session)(nil)
	var _ nativeLeg = (*quiclink.Session)(nil)
	for _, tc := range []struct {
		name string
		err  error
		want *link.Abort
	}{
		{"connection lost", &link.Abort{Code: link.CodeConnLost, Msg: "link: connection closed"}, dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, NF1NotServed)},
		{"draining", &link.Abort{Code: link.CodeDraining}, dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, NF1NotServed)},
		{"connection ended", link.ErrConnClosed, dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_AGENT_OFFLINE, NF1NotServed)},
		{"the agent's refusal", dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "busy"), dataplane.Refusal(relayv1.ErrorReason_ERROR_REASON_LIMIT_EXCEEDED, "busy")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := nf1Leg(answerLeg{err: tc.err})
			cs, ok := s.(peerlink.ChainSession)
			if !ok {
				t.Fatal("the leg is no ChainSession")
			}
			if _, ok := s.(peerlink.EndAcker); !ok {
				t.Fatal("the leg hides its HoldEndAck and AckEnd")
			}
			if _, ok := s.(interface{ CloseWriteSyncReceived(func()) error }); !ok {
				t.Fatal("the leg hides its CloseWriteSyncReceived")
			}
			var a *link.Abort
			if err := cs.Answered(t.Context()); !errors.As(err, &a) || a.Code != tc.want.Code || a.Msg != tc.want.Msg || !a.Refused {
				t.Fatalf("Answered: %v, want %v", err, tc.want)
			}
		})
	}
	if err := nf1Leg(answerLeg{}).(peerlink.ChainSession).Answered(t.Context()); err != nil {
		t.Fatalf("an answered leg: %v", err)
	}
}
