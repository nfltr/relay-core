// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A draining replica lets a TCP agent go within the first half of the
// shutdown window (it redials another replica), not at the forced stop.
func TestConnectTCPLeavesWhenDraining(t *testing.T) {
	global, drain := context.WithCancel(context.Background())
	const window = 400 * time.Millisecond
	d := &Dispatcher{TCPDispatcher: NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, nil), Logger: slog.New(slog.DiscardHandler)}
	d.TCPDispatcher.ConfigureDrain(global.Done(), window) // as server_run.go
	st := &fake.EndableStream{End: make(chan struct{}), Ctx: makeGRPCContext("drain-agent")}
	t.Cleanup(func() { close(st.End) })
	done := make(chan error, 1)
	go func() { done <- d.ConnectTCP(st) }()
	waitStreamsOf(t, d.TCPDispatcher, "drain-agent", 1)

	start := time.Now()
	drain()
	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("left with %v, want Unavailable (server restarting)", err)
		}
		if took := time.Since(start); took > window/2+200*time.Millisecond {
			t.Fatalf("left after %v, want within half of the %v window", took, window)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the TCP agent was never let go while draining")
	}
}

func TestDrainJitterWithinFirstHalf(t *testing.T) {
	for range 1000 {
		if j := drainJitter(time.Second); j < 0 || j >= 500*time.Millisecond {
			t.Fatalf("jitter %v outside [0, 500ms)", j)
		}
	}
	if drainJitter(0) != 0 {
		t.Fatal("no window: leave at once")
	}
}
