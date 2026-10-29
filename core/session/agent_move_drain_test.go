// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

// TestMoveAskedAtDrainStart: a leaving replica asks an agent that moves
// to move as the drain starts, not at a jittered moment of the window
// (which kept the agent, and peers' sessions to it, on the leaving replica
// for up to half the window).
func TestMoveAskedAtDrainStart(t *testing.T) {
	d, s, sink, _, inv, drain := movingSession(t)
	d.gracefulShutdownTimeout = 10 * time.Minute
	ctx, stop := context.WithCancel(t.Context())
	watched := make(chan struct{})
	go func() { defer close(watched); _ = d.WatchAgentSession(ctx, "agent-1", s, inv, make(chan error)) }()
	defer func() { stop(); <-watched }() // the watcher ends with the test
	drain()
	start := time.Now()
	for len(sink.steps()) == 0 {
		if time.Since(start) > time.Second {
			t.Fatal("no MOVE_RECONNECT within a second of the drain's start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := sink.steps()[0]; got != pb.Move_MOVE_RECONNECT {
		t.Fatalf("sent %v, want MOVE_RECONNECT", got)
	}
}

// TestAgentLinkRunsToDrainDeadline: an agent's sessions on its agent link,
// over TCP and over QUIC, run on when the relay's drain starts (Run's ctx
// ends: QUIC used to be drained at once there) and when its gRPC port
// closes (TCP used to be drained at once there), and are aborted
// CodeDrainDeadline at the drain's deadline, not before.
func TestAgentLinkRunsToDrainDeadline(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ag        harnessAgent
		transport string
	}{{"tcp", agentCurrent, dataplane.AgentLinkTCP}, {"quic", agentQUIC, dataplane.AgentLinkQUIC}} {
		t.Run(tc.name, func(t *testing.T) {
			h := startTunnelHarnessWith(t, startTarget(t), nil, "", 0, relayCurrent, tc.ag)
			c := h.Dial(t)
			t.Cleanup(func() { _ = c.Close() })
			echo := func(msg string) error {
				if _, err := c.Write([]byte(msg)); err != nil {
					return err
				}
				got := make([]byte, len(msg))
				_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
				_, err := io.ReadFull(c, got)
				if err == nil && !bytes.Equal(got, []byte(msg)) {
					err = errors.New("echo " + string(got))
				}
				return err
			}
			if _, err := c.Write(targetHeader('e', 0)); err != nil {
				t.Fatal(err)
			}
			if err := echo("before"); err != nil {
				t.Fatal(err)
			}
			h.D.Link.Mu.Lock()
			var on bool
			for _, conns := range h.D.Link.Conns {
				for _, lc := range conns {
					on = on || lc.Transport == tc.transport
				}
			}
			h.D.Link.Mu.Unlock()
			if !on {
				t.Fatalf("no %s agent-link connection bound", tc.transport)
			}

			deadline := time.Now().Add(3 * time.Second)
			h.D.DrainBy = func() time.Time { return deadline }
			if h.StopQUIC != nil {
				h.StopQUIC() // the drain starts
			}
			time.Sleep(200 * time.Millisecond)
			if err := echo("drain started"); err != nil {
				t.Fatalf("after the drain started: %v", err)
			}
			go h.Srv.GracefulStop() // the gRPC port closes
			time.Sleep(200 * time.Millisecond)
			if err := echo("port closed"); err != nil {
				t.Fatalf("after the gRPC port closed, %v before the deadline: %v", time.Until(deadline), err)
			}
			_ = c.SetReadDeadline(deadline.Add(5 * time.Second))
			_, err := c.Read(make([]byte, 1))
			if err == nil {
				t.Fatal("the session outlived the drain's deadline")
			}
			if early := time.Until(deadline); early > 100*time.Millisecond {
				t.Fatalf("the session ended %v before the deadline: %v", early, err)
			}
		})
	}
}
