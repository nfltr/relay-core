// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connect

import (
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/link"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// localEnd is a link session as its own side sees it after its AbortWith
// or Reset: a Read returns io.EOF, a Write io.ErrClosedPipe (link
// Session.end, writeErr).
type localEnd struct {
	net.Conn
	once sync.Once
	done chan struct{}
}

func newLocalEnd() *localEnd { return &localEnd{done: make(chan struct{})} }

func (s *localEnd) Read([]byte) (int, error) { <-s.done; return 0, io.EOF }
func (s *localEnd) Write(p []byte) (int, error) {
	select {
	case <-s.done:
		return 0, io.ErrClosedPipe
	default:
		return len(p), nil
	}
}
func (s *localEnd) end()                        { s.once.Do(func() { close(s.done) }) }
func (s *localEnd) AbortWith(*link.Abort) error { s.end(); return nil }
func (s *localEnd) Reset() error                { s.end(); return nil }
func (s *localEnd) CloseWrite() error           { return nil }
func (s *localEnd) Close() error                { s.end(); return nil }

// TestBudgetedLegEndsTyped: once the relay cut a budgeted leg, its Read and
// Write report the cut's ERROR_REASON_ACCOUNT_REQUIRED, never the clean end
// the aborted onward session reads locally: the join passes what they
// return to the other end, and a clean end there reached the client ahead
// of the cut's own abort (TestNF1AnonLegTime: "cut in transit"; spec
// NF1Anon.tla JoinEnd, mutant join-clean-end).
func TestBudgetedLegEndsTyped(t *testing.T) {
	leg := nf1anon.New(nf1anon.Settings{Enabled: true, LegBytes: 1 << 20, LegTime: time.Millisecond, BucketBytes: 1 << 20}, nil).Leg("nf1:00")
	s := budgeted(newLocalEnd(), newLocalEnd(), leg)
	typed := func(err error) bool {
		var a *link.Abort
		return errors.As(err, &a) && a.Code == dataplane.AbortCode(nf1anon.Reason)
	}
	if _, err := s.Read(make([]byte, 1)); !typed(err) { // ends at the leg's time
		t.Fatalf("Read after the cut: %v, want ERROR_REASON_ACCOUNT_REQUIRED", err)
	}
	if _, err := s.Write([]byte{1}); !typed(err) {
		t.Fatalf("Write after the cut: %v, want ERROR_REASON_ACCOUNT_REQUIRED", err)
	}
}
