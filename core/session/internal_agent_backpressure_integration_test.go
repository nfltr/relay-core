// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay/client/rpcclient"
)

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
			for i := range 257 {
				if err := stream.Send(fmt.Appendf(nil, "stalled-%d", i)); err != nil {
					return err
				}
			}
		case "healthy":
			for _, message := range []string{"task.accepted", "task.progress:1", "task.progress:2"} {
				if err := stream.Send([]byte(message)); err != nil {
					return err
				}
			}
		}
		return nil
	})

	stalled, err := source.NewStream(ctx, targetLoop.AgentID())
	if err != nil {
		t.Fatalf("NewStream(stalled): %v", err)
	}
	if err := stalled.Send([]byte("saturate")); err != nil {
		t.Fatalf("stalled Send: %v", err)
	}
	if err := stalled.CloseSend(); err != nil {
		t.Fatalf("stalled CloseSend: %v", err)
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
}
