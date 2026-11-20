// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/link/peerlink"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay-core/platform/metrics"
)

// endRecorder is a link or quiclink session as a joint sees it (every end
// method peerlink.Join uses), recording the end calls a wrapper passes on.
type endRecorder struct {
	net.Conn
	calls []string
}

func (e *endRecorder) rec(m string) error             { e.calls = append(e.calls, m); return nil }
func (e *endRecorder) CloseWrite() error              { return e.rec("CloseWrite") }
func (e *endRecorder) CloseWriteSync() error          { return e.rec("CloseWriteSync") }
func (e *endRecorder) Reset() error                   { return e.rec("Reset") }
func (e *endRecorder) HoldEndAck()                    { _ = e.rec("HoldEndAck") }
func (e *endRecorder) AckEnd()                        { _ = e.rec("AckEnd") }
func (e *endRecorder) Close() error                   { return e.rec("Close") }
func (e *endRecorder) Done() <-chan struct{}          { return nil }
func (e *endRecorder) Answers() bool                  { return true }
func (e *endRecorder) Accept() error                  { return nil }
func (e *endRecorder) AcceptPath(uint8) error         { return nil }
func (e *endRecorder) Refuse(uint32, string) error    { return nil }
func (e *endRecorder) AbortWith(*link.Abort) error    { return e.rec("AbortWith") }
func (e *endRecorder) Answered(context.Context) error { return nil }
func (e *endRecorder) Chain() link.Chain              { return link.Chain{} }
func (e *endRecorder) PathLen() uint8                 { return 0 }
func (e *endRecorder) CloseWriteSyncReceived(received func()) error {
	received()
	return e.rec("CloseWriteSyncReceived")
}

// TestSessionWrappersPassTheEndOn (lane QUICEND; relay's AnswerConn class,
// relay client/nf1 TestAnswerConnPassesTheSessionsEnd): every wrapper the
// relay puts over a link or quiclink session on a join's path passes its
// end on: CloseWrite, CloseWriteSync, CloseWriteSyncReceived, Reset, and
// the joint's hold (HoldEndAck, AckEnd). Hidden, the joint half-closes
// without waiting for the far end and never holds the end: an anonymous
// nf1 leg (budgeted) did. core/session TestNF1LegAnswersNotServedWhenTheAgentLeft
// checks nf1Leg, platform/metrics TestCountConnPassesTheEndHandshake
// CountConn on its own.
func TestSessionWrappersPassTheEndOn(t *testing.T) {
	leg := func() *nf1anon.Leg {
		return nf1anon.New(nf1anon.Settings{Enabled: true, LegBytes: 1 << 20, LegTime: time.Hour, BucketBytes: 1 << 20}, nil).Leg("nf1:00")
	}
	for _, tc := range []struct {
		name string
		wrap func(*endRecorder) net.Conn
	}{
		{"budgeted", func(e *endRecorder) net.Conn { return budgeted(newLocalEnd(), e, leg()) }},
		{"budgeted, counted", func(e *endRecorder) net.Conn {
			return metrics.CountConn(budgeted(newLocalEnd(), e, leg()), metrics.PathConnect, nil, time.Now(), true, true)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := &endRecorder{}
			s, ok := tc.wrap(e).(interface {
				CloseWrite() error
				CloseWriteSync() error
				CloseWriteSyncReceived(func()) error
				Reset() error
				peerlink.EndAcker
			})
			if !ok {
				t.Fatal("the wrapper hides its session's CloseWrite, CloseWriteSync, CloseWriteSyncReceived, Reset, HoldEndAck or AckEnd")
			}
			_ = s.CloseWrite()
			_ = s.CloseWriteSync()
			got := false
			_ = s.CloseWriteSyncReceived(func() { got = true })
			s.HoldEndAck()
			s.AckEnd()
			_ = s.Reset()
			want := []string{"CloseWrite", "CloseWriteSync", "CloseWriteSyncReceived", "HoldEndAck", "AckEnd", "Reset"}
			if !got || !slices.Equal(e.calls, want) {
				t.Fatalf("passed on %v (receipt %v), want %v", e.calls, got, want)
			}
		})
	}
}
