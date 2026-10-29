// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"testing/synctest"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

func TestSendGRPCHeartbeats_RefreshesEndpointLastSeen(t *testing.T) {
	d, cancelDispatcher := newTestDispatcher(t)
	defer cancelDispatcher()
	d.Config.HeartbeatSendDuration = 10 * time.Millisecond

	heartbeatSeen := make(chan string, 1)
	d.OnAgentHeartbeat = func(agentID string) {
		select {
		case heartbeatSeen <- agentID:
		default:
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &fake.MockAgentTunnelServer{Ctx: ctx}
	agentStream := NewGRPCStream("agent-heartbeat", stream, ctx, make(chan proto.Message, 1), time.NewTicker(time.Second))
	t.Cleanup(func() {
		agentStream.heartbeatsRecvThresholdTicker.Stop()
	})

	done := make(chan error, 1)
	go func() {
		done <- d.sendGRPCHeartbeats(ctx, "agent-heartbeat", agentStream)
	}()

	select {
	case agentID := <-heartbeatSeen:
		if agentID != "agent-heartbeat" {
			t.Fatalf("agentID = %q, want %q", agentID, "agent-heartbeat")
		}
	case <-time.After(time.Second):
		cancel()
		t.Fatal("timed out waiting for heartbeat refresh callback")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("sendGRPCHeartbeats error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sendGRPCHeartbeats to stop")
	}

	if len(stream.Sent) == 0 {
		t.Fatal("expected at least one heartbeat frame to be sent")
	}
}

func TestSendGRPCHeartbeats_SkipsWhenOutboundDataRecentlySent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &Dispatcher{
			Config: DispatcherConfig{
				HeartbeatSendDuration: 50 * time.Millisecond,
			},
			Logger:              fake.TestLogger(),
			StreamerIPInventory: directory.NewNoOpInventory(),
		}

		ctx, cancel := context.WithCancel(context.Background())
		stream := &fake.MockAgentTunnelServer{Ctx: ctx}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		agentStream := NewGRPCStream("agent-heartbeat-data", stream, ctx, make(chan proto.Message, 1), ticker)

		done := make(chan error, 1)
		go func() {
			done <- d.sendGRPCHeartbeats(ctx, "agent-heartbeat-data", agentStream)
		}()

		time.Sleep(20 * time.Millisecond)
		if err := agentStream.Send(&pb.TunnelFrame{TransactionId: "txn-data", Payload: []byte("data")}); err != nil {
			t.Fatalf("send data frame: %v", err)
		}

		time.Sleep(45 * time.Millisecond)
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("sendGRPCHeartbeats error = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("sendGRPCHeartbeats did not stop after cancellation")
		}

		for _, msg := range stream.Sent {
			if msg.GetTransactionId() == "" && len(msg.GetPayload()) == 0 && !msg.GetEom() {
				t.Fatalf("heartbeat was sent despite recent outbound data; sent=%d", len(stream.Sent))
			}
		}
	})
}

func TestSendGRPCHeartbeats_DoesNotTreatHeartbeatAsOutboundData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := &Dispatcher{
			Config: DispatcherConfig{
				HeartbeatSendDuration: 50 * time.Millisecond,
			},
			Logger:              fake.TestLogger(),
			StreamerIPInventory: directory.NewNoOpInventory(),
		}

		ctx, cancel := context.WithCancel(context.Background())
		stream := &fake.MockAgentTunnelServer{Ctx: ctx}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		agentStream := NewGRPCStream("agent-heartbeat-idle", stream, ctx, make(chan proto.Message, 1), ticker)

		done := make(chan error, 1)
		go func() {
			done <- d.sendGRPCHeartbeats(ctx, "agent-heartbeat-idle", agentStream)
		}()

		time.Sleep(110 * time.Millisecond)
		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("sendGRPCHeartbeats error = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("sendGRPCHeartbeats did not stop after cancellation")
		}

		heartbeats := 0
		for _, msg := range stream.Sent {
			if msg.GetTransactionId() == "" && len(msg.GetPayload()) == 0 && !msg.GetEom() {
				heartbeats++
			}
		}
		if heartbeats < 2 {
			t.Fatalf("heartbeats sent = %d, want at least 2", heartbeats)
		}
	})
}

// ---- mock RelayTunnelServer ----
