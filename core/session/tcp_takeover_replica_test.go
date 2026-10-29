// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/platform/tcprig"
)

// A rolling update's new pod reached another replica than the old pod
// (two replicas, one directory): the old replica's renewal of the old TCP
// connection is superseded. That replica keeps the sessions open through the
// replaced connection until they end, and routes no new open to it (they go
// where the directory points: the new connection). Before: the stream ended
// at its next renewal and cut every open session.
func TestTCPTakeoverOnAnotherReplicaLetsOpenSessionsFinish(t *testing.T) {
	for name, ag := range map[string]harnessAgent{"agent link": agentCurrent, "data connections": agentDataConns, "one connection": agentOneConn} {
		t.Run(name, func(t *testing.T) { testTCPTakeoverOnAnotherReplica(t, ag) })
	}
}

func testTCPTakeoverOnAnotherReplica(t *testing.T, ag harnessAgent) {
	var taken atomic.Bool // the other replica's session holds the record
	o := rigOptions
	o.New = func(logger *slog.Logger, auxIdle time.Duration) *TCPDispatcher {
		d := rigOptions.New(logger, auxIdle)
		d.ConfigureRenew(20*time.Millisecond, func(string, directory.Session) error {
			if taken.Load() {
				return directory.ErrSuperseded
			}
			return nil
		}, nil)
		return d
	}
	h := tcprig.StartTunnelHarnessWith(t, o, startTarget(t), nil, "", 0, relayCurrent, ag, 0)
	c := h.Dial(t)
	defer func() { _ = c.Close() }()
	if _, err := c.Write(targetHeader('e', 0)); err != nil {
		t.Fatal(err)
	}
	roundTrip := func(what string) error {
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Write([]byte(what)); err != nil {
			return err
		}
		got := make([]byte, len(what))
		if _, err := io.ReadFull(c, got); err != nil {
			return err
		}
		if string(got) != what {
			t.Fatalf("echoed %q, want %q", got, what)
		}
		return nil
	}
	if err := roundTrip("before"); err != nil {
		t.Fatal(err)
	}

	old := h.D.agentStream(harnessAgentID)
	taken.Store(true)
	took := time.Now()
	for n := 0; time.Since(took) < time.Second; n++ { // fifty renewals
		if err := roundTrip("during"); err != nil {
			t.Fatalf("the session open through the replaced connection broke %v after the takeover (%d echoes): %v",
				time.Since(took).Round(time.Millisecond), n, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if as := h.D.agentStream(harnessAgentID); as != nil {
		t.Fatal("new opens on this replica still go to the replaced connection")
	}
	select {
	case <-old.Done:
		t.Fatal("the replaced connection's stream ended while a session was open")
	default:
	}

	_ = c.Close() // its client ends it: the replaced connection goes with it
	select {
	case <-old.Done:
	case <-time.After(5 * time.Second):
		t.Fatal("the replaced connection's stream stayed after its last session ended")
	}
}
