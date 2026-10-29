// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
)

func TestInternalAgentLoopback_DispatcherToAgent(t *testing.T) {
	loop := NewInternalAgentLoopback(context.Background(), "test-agent")
	defer loop.Close()

	ds := loop.DispatcherSide()
	as := loop.AgentSide()

	frame := &pb.TunnelFrame{TransactionId: "tx-1", MessageType: "test-request"}
	if err := ds.Send(frame); err != nil {
		t.Fatalf("dispatcher Send: %v", err)
	}
	got, err := as.Recv()
	if err != nil {
		t.Fatalf("agent Recv: %v", err)
	}
	if got.GetTransactionId() != "tx-1" {
		t.Fatalf("txn id = %q, want tx-1", got.GetTransactionId())
	}
}

func TestInternalAgentLoopback_AgentToDispatcher(t *testing.T) {
	loop := NewInternalAgentLoopback(context.Background(), "test-agent")
	defer loop.Close()

	ds := loop.DispatcherSide()
	as := loop.AgentSide()

	frame := &pb.TunnelFrame{TransactionId: "tx-2", MessageType: "test-response"}
	if err := as.Send(frame); err != nil {
		t.Fatalf("agent Send: %v", err)
	}
	got, err := ds.Recv()
	if err != nil {
		t.Fatalf("dispatcher Recv: %v", err)
	}
	if got.GetTransactionId() != "tx-2" {
		t.Fatalf("txn id = %q, want tx-2", got.GetTransactionId())
	}
}

func TestInternalAgentLoopback_CloseTerminatesRecv(t *testing.T) {
	loop := NewInternalAgentLoopback(context.Background(), "test-agent")
	ds := loop.DispatcherSide()
	as := loop.AgentSide()

	errCh := make(chan error, 2)
	go func() {
		_, err := ds.Recv()
		errCh <- err
	}()
	go func() {
		_, err := as.Recv()
		errCh <- err
	}()

	// Allow goroutines to enter Recv.
	time.Sleep(50 * time.Millisecond)
	loop.Close()

	for range 2 {
		select {
		case err := <-errCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Recv after close = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Recv did not return after Close")
		}
	}
}

func TestInternalAgentLoopback_ParentCancelTerminates(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	loop := NewInternalAgentLoopback(parent, "test-agent")
	ds := loop.DispatcherSide()

	done := make(chan error, 1)
	go func() {
		_, err := ds.Recv()
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recv after parent cancel = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv did not return after parent cancel")
	}
}

func TestInternalAgentLoopback_AgentID(t *testing.T) {
	loop := NewInternalAgentLoopback(context.Background(), "nfltr-relay-orchestrator")
	defer loop.Close()
	if got := loop.AgentID(); got != "nfltr-relay-orchestrator" {
		t.Fatalf("AgentID = %q, want nfltr-relay-orchestrator", got)
	}
}

func TestInternalAgentLoopback_DispatcherStreamerContext(t *testing.T) {
	loop := NewInternalAgentLoopback(context.Background(), "test-agent")
	defer loop.Close()
	ds := loop.DispatcherSide()
	if ds.Context() == nil {
		t.Fatal("DispatcherSide Context returned nil")
	}
}

func TestInternalAgentLoopback_DispatcherSendFailsWhenQueueIsSaturated(t *testing.T) {
	loop := NewInternalAgentLoopback(context.Background(), "test-agent")
	defer loop.Close()
	ds := loop.DispatcherSide()

	for i := range internalAgentLoopbackBuffer {
		if err := ds.Send(&pb.TunnelFrame{TransactionId: fmt.Sprintf("tx-%d", i)}); err != nil {
			t.Fatalf("fill send %d: %v", i, err)
		}
	}

	started := time.Now()
	err := ds.Send(&pb.TunnelFrame{TransactionId: "overflow"})
	if !errors.Is(err, errInternalAgentLoopbackBackpressure) {
		t.Fatalf("overflow send = %v, want %v", err, errInternalAgentLoopbackBackpressure)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("overflow send took %v, want bounded failure", elapsed)
	}
}
