// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay/client/rpcclient"
)

// The source agent's receive loop serves both calls. The stalled call's
// handler sends past the caller's window and waits for credit the caller
// never grants; only that call waits. The healthy call starts only once
// the stalled handler waits (the order a loaded machine took before lane
// CALLCREDIT, when the source's loop then held for handoff.Stall, 10 s)
// and still completes at once. When the stalled caller leaves, its handler
// is told (a cancel) and returns.
func TestInternalAgentRouting_SaturatedTransactionDoesNotBlockHealthyTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	d := newTestDispatcherForInternalAgent(t)
	sourceLoop := NewInternalAgentLoopback(ctx, "source-agent")
	targetLoop := NewInternalAgentLoopback(ctx, "target-agent")
	defer sourceLoop.Close()
	defer targetLoop.Close()
	cleanupSource := d.RegisterInternalAgent(ctx, sourceLoop.AgentID(), sourceLoop.DispatcherSide())
	cleanupTarget := d.RegisterInternalAgent(ctx, targetLoop.AgentID(), targetLoop.DispatcherSide())
	defer cleanupSource()
	defer cleanupTarget()

	source := rpcclient.NewAgent(sourceLoop.AgentID(), sourceLoop.AgentSide())
	target := rpcclient.NewAgent(targetLoop.AgentID(), targetLoop.AgentSide())
	defer source.Close() //nolint:errcheck
	defer target.Close() //nolint:errcheck

	var sent atomic.Int64
	waiting := make(chan struct{})    // the stalled handler sent its window
	stalledEnd := make(chan error, 1) // its Send's error once the caller left
	target.Handle(func(stream rpcclient.ServerStream) error {
		request, err := stream.Recv()
		if err != nil {
			return err
		}
		if _, err := stream.Recv(); err != io.EOF {
			return fmt.Errorf("request close = %v, want EOF", err)
		}
		switch string(request) {
		case "saturate":
			for i := range rpcclient.ResponseWindow + 1 {
				if sent.Add(1) == rpcclient.ResponseWindow+1 {
					close(waiting)
				}
				if err := stream.Send(fmt.Appendf(nil, "stalled-%d", i)); err != nil {
					stalledEnd <- err
					return err
				}
			}
			stalledEnd <- nil
		case "healthy":
			for _, message := range []string{"task.accepted", "task.progress:1", "task.progress:2"} {
				if err := stream.Send([]byte(message)); err != nil {
					return err
				}
			}
		}
		return nil
	})

	stalledCtx, leave := context.WithCancel(ctx)
	defer leave()
	stalled, err := source.NewStream(stalledCtx, targetLoop.AgentID())
	if err != nil {
		t.Fatalf("NewStream(stalled): %v", err)
	}
	if err := stalled.Send([]byte("saturate")); err != nil {
		t.Fatalf("stalled Send: %v", err)
	}
	if err := stalled.CloseSend(); err != nil {
		t.Fatalf("stalled CloseSend: %v", err)
	}
	select {
	case <-waiting:
	case <-ctx.Done():
		t.Fatalf("the stalled handler sent %d of its window", sent.Load())
	}

	healthy, err := source.NewStream(ctx, targetLoop.AgentID())
	if err != nil {
		t.Fatalf("NewStream(healthy): %v", err)
	}
	if err := healthy.Send([]byte("healthy")); err != nil {
		t.Fatalf("healthy Send: %v", err)
	}
	if err := healthy.CloseSend(); err != nil {
		t.Fatalf("healthy CloseSend: %v", err)
	}

	for i, want := range []string{"task.accepted", "task.progress:1", "task.progress:2"} {
		response, err := healthy.Recv()
		if err != nil {
			t.Fatalf("healthy Recv(%d): %v", i, err)
		}
		if got := string(response.Data); got != want {
			t.Fatalf("healthy Recv(%d) = %q, want %q", i, got, want)
		}
	}
	if _, err := healthy.Recv(); err != io.EOF {
		t.Fatalf("healthy terminal Recv = %v, want EOF", err)
	}

	leave()
	select {
	case err := <-stalledEnd:
		if err == nil {
			t.Fatal("the stalled handler sent past the caller's window")
		}
	case <-ctx.Done():
		t.Fatal("the stalled handler still waits after its caller left")
	}
}

// A call's responses past the caller's window flow while the caller reads:
// its window updates travel the request direction, which its CloseSend
// ended, and the relay forwards them after that end (relay spec/CallCredit.tla
// drop-after-eom). Requests past the handler's window flow back the same
// way on the response direction.
func TestInternalAgentRouting_StreamsPastTheWindows(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		d := newTestDispatcherForInternalAgent(t)
		streamPastTheWindows(t, d, d)
	})
	t.Run("cross-replica", func(t *testing.T) { // routeAgentToAgentRemote -> RelayTunnel
		remoteClient, remote, cleanup := newA2ABufConnEnv(t)
		defer cleanup()
		source := newTestDispatcherForInternalAgent(t)
		source.PodDialer = &relayOnlyPodDialer{client: remoteClient, ctx: t.Context()}
		streamPastTheWindows(t, source, remote)
	})
}

// streamPastTheWindows runs a call from an agent on sourceD to one on
// targetD with three windows of requests and of responses.
func streamPastTheWindows(t *testing.T, sourceD, targetD *Dispatcher) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // a hang bound
	defer cancel()

	sourceLoop := NewInternalAgentLoopback(ctx, "source-agent")
	targetLoop := NewInternalAgentLoopback(ctx, "target-agent")
	defer sourceLoop.Close()
	defer targetLoop.Close()
	defer sourceD.RegisterInternalAgent(ctx, sourceLoop.AgentID(), sourceLoop.DispatcherSide())()
	defer targetD.RegisterInternalAgent(ctx, targetLoop.AgentID(), targetLoop.DispatcherSide())()

	source := rpcclient.NewAgent(sourceLoop.AgentID(), sourceLoop.AgentSide())
	target := rpcclient.NewAgent(targetLoop.AgentID(), targetLoop.AgentSide())
	defer source.Close() //nolint:errcheck
	defer target.Close() //nolint:errcheck

	const requests, responses = 3 * rpcclient.RequestWindow, 3 * rpcclient.ResponseWindow
	target.Handle(func(stream rpcclient.ServerStream) error {
		for i := 0; ; i++ {
			request, err := stream.Recv()
			if err == io.EOF {
				if i != requests {
					return fmt.Errorf("got %d requests, want %d", i, requests)
				}
				break
			}
			if err != nil {
				return err
			}
			if got := string(request); got != fmt.Sprint(i) {
				return fmt.Errorf("request %d = %q", i, got)
			}
		}
		for i := range responses {
			if err := stream.Send(fmt.Append(nil, i)); err != nil {
				return err
			}
		}
		return nil
	})

	call, err := source.NewStream(ctx, targetLoop.AgentID())
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	for i := range requests {
		if err := call.Send(fmt.Append(nil, i)); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}
	if err := call.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	for i := range responses {
		response, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv(%d): %v", i, err)
		}
		if got := string(response.Data); got != fmt.Sprint(i) {
			t.Fatalf("Recv(%d) = %q", i, got)
		}
	}
	if _, err := call.Recv(); err != io.EOF {
		t.Fatalf("terminal Recv = %v, want EOF", err)
	}
}
