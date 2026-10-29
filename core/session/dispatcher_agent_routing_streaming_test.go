// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay/client/rpcclient"
	"google.golang.org/grpc/metadata"
)

func TestDispatcher_A2ALocal_MultiResponseAfterCloseSend(t *testing.T) {
	t.Parallel()

	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	targetCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "worker-target",
		"x-auth-method", "apikey",
	))
	targetTunnel, err := client.AgentTunnel(targetCtx)
	if err != nil {
		t.Fatalf("target AgentTunnel: %v", err)
	}
	target := rpcclient.NewAgent("worker-target", targetTunnel)
	awaitUpstreamRegistered(t, d, "worker-target")
	defer target.Close() //nolint:errcheck

	target.Handle(func(stream rpcclient.ServerStream) error {
		data, err := stream.Recv()
		if err != nil {
			return err
		}
		if string(data) != dispatcherRoutingRequestPayload {
			t.Fatalf("stream.Recv() = %q, want request", string(data))
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Fatalf("second stream.Recv() error = %v, want io.EOF", err)
		}
		if err := stream.Send([]byte("accepted")); err != nil {
			return err
		}
		time.Sleep(250 * time.Millisecond)
		if err := stream.Send([]byte("result")); err != nil {
			return err
		}
		return nil
	})

	sourceCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "planner-source",
		"x-auth-method", "apikey",
	))
	sourceTunnel, err := client.AgentTunnel(sourceCtx)
	if err != nil {
		t.Fatalf("source AgentTunnel: %v", err)
	}
	source := rpcclient.NewAgent("planner-source", sourceTunnel)
	defer source.Close() //nolint:errcheck

	stream, err := source.NewStream(ctx, "worker-target")
	if err != nil {
		t.Fatalf("NewStream() error = %v", err)
	}
	if err := stream.Send([]byte(dispatcherRoutingRequestPayload)); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv() error = %v", err)
	}
	if string(first.Data) != "accepted" {
		t.Fatalf("first response = %q, want accepted", string(first.Data))
	}

	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second Recv() error = %v", err)
	}
	if string(second.Data) != "result" {
		t.Fatalf("second response = %q, want result", string(second.Data))
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("final Recv() error = %v, want io.EOF", err)
	}
}

func TestDispatcher_A2ALocal_MultiResponseAfterCloseSend_E2EE(t *testing.T) {
	t.Parallel()

	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	targetCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "worker-target-e2ee",
		"x-auth-method", "apikey",
	))
	targetTunnel, err := client.AgentTunnel(targetCtx)
	if err != nil {
		t.Fatalf("target AgentTunnel: %v", err)
	}
	target := rpcclient.NewAgent("worker-target-e2ee", targetTunnel)
	awaitUpstreamRegistered(t, d, "worker-target-e2ee")
	target.EnableA2AE2EE()
	defer target.Close() //nolint:errcheck

	target.Handle(func(stream rpcclient.ServerStream) error {
		data, err := stream.Recv()
		if err != nil {
			return err
		}
		if string(data) != dispatcherRoutingRequestPayload {
			t.Fatalf("stream.Recv() = %q, want request", string(data))
		}
		if _, err := stream.Recv(); err != io.EOF {
			t.Fatalf("second stream.Recv() error = %v, want io.EOF", err)
		}
		if err := stream.Send([]byte("accepted")); err != nil {
			return err
		}
		time.Sleep(250 * time.Millisecond)
		if err := stream.Send([]byte(`{"status":"success","patch":"diff --git a/x b/x\n+payload"}`)); err != nil {
			return err
		}
		return nil
	})

	sourceCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "planner-source-e2ee",
		"x-auth-method", "apikey",
	))
	sourceTunnel, err := client.AgentTunnel(sourceCtx)
	if err != nil {
		t.Fatalf("source AgentTunnel: %v", err)
	}
	source := rpcclient.NewAgent("planner-source-e2ee", sourceTunnel)
	source.EnableA2AE2EE()
	defer source.Close() //nolint:errcheck

	stream, err := source.NewStream(ctx, "worker-target-e2ee")
	if err != nil {
		t.Fatalf("NewStream() error = %v", err)
	}
	if err := stream.Send([]byte(dispatcherRoutingRequestPayload)); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend() error = %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv() error = %v", err)
	}
	if string(first.Data) != "accepted" {
		t.Fatalf("first response = %q, want accepted", string(first.Data))
	}

	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second Recv() error = %v", err)
	}
	if string(second.Data) != `{"status":"success","patch":"diff --git a/x b/x\n+payload"}` {
		t.Fatalf("second response = %q, want encrypted result payload", string(second.Data))
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("final Recv() error = %v, want io.EOF", err)
	}
}

func TestDispatcher_A2ALocal_MultiResponseWhileTargetRecvBlocked_E2EE(t *testing.T) {
	t.Parallel()

	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	targetCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "worker-target-blocked-e2ee",
		"x-auth-method", "apikey",
	))
	targetTunnel, err := client.AgentTunnel(targetCtx)
	if err != nil {
		t.Fatalf("target AgentTunnel: %v", err)
	}
	target := rpcclient.NewAgent("worker-target-blocked-e2ee", targetTunnel)
	awaitUpstreamRegistered(t, d, "worker-target-blocked-e2ee")
	target.EnableA2AE2EE()
	defer target.Close() //nolint:errcheck

	recvDone := make(chan error, 1)
	target.Handle(func(stream rpcclient.ServerStream) error {
		data, err := stream.Recv()
		if err != nil {
			return err
		}
		if string(data) != dispatcherRoutingRequestPayload {
			t.Fatalf("stream.Recv() = %q, want request", string(data))
		}

		go func() {
			_, recvErr := stream.Recv()
			recvDone <- recvErr
		}()

		if err := stream.Send([]byte("accepted")); err != nil {
			return err
		}
		time.Sleep(250 * time.Millisecond)
		if err := stream.Send([]byte(`{"status":"success","patch":"diff --git a/x b/x\n+payload"}`)); err != nil {
			return err
		}
		return nil
	})

	sourceCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "planner-source-blocked-e2ee",
		"x-auth-method", "apikey",
	))
	sourceTunnel, err := client.AgentTunnel(sourceCtx)
	if err != nil {
		t.Fatalf("source AgentTunnel: %v", err)
	}
	source := rpcclient.NewAgent("planner-source-blocked-e2ee", sourceTunnel)
	source.EnableA2AE2EE()
	defer source.Close() //nolint:errcheck

	stream, err := source.NewStream(ctx, "worker-target-blocked-e2ee")
	if err != nil {
		t.Fatalf("NewStream() error = %v", err)
	}
	if err := stream.Send([]byte(dispatcherRoutingRequestPayload)); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	first, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv() error = %v", err)
	}
	if string(first.Data) != "accepted" {
		t.Fatalf("first response = %q, want accepted", string(first.Data))
	}

	second, err := stream.Recv()
	if err != nil {
		t.Fatalf("second Recv() error = %v", err)
	}
	if string(second.Data) != `{"status":"success","patch":"diff --git a/x b/x\n+payload"}` {
		t.Fatalf("second response = %q, want encrypted result payload", string(second.Data))
	}

	select {
	case recvErr := <-recvDone:
		if !errors.Is(recvErr, context.Canceled) && recvErr != io.EOF {
			t.Fatalf("background Recv() error = %v, want context.Canceled or io.EOF", recvErr)
		}
	case <-ctx.Done():
		t.Fatal("background Recv() did not unblock")
	}
}

func TestDispatcher_A2ALocal_ManyResponsesWhileTargetRecvBlocked_E2EE(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	targetCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "worker-target-many-blocked-e2ee",
		"x-auth-method", "apikey",
	))
	targetTunnel, err := client.AgentTunnel(targetCtx)
	if err != nil {
		t.Fatalf("target AgentTunnel: %v", err)
	}
	target := rpcclient.NewAgent("worker-target-many-blocked-e2ee", targetTunnel)
	awaitUpstreamRegistered(t, d, "worker-target-many-blocked-e2ee")
	target.EnableA2AE2EE()
	defer target.Close() //nolint:errcheck

	recvDone := make(chan error, 1)
	responses := []string{
		"accepted",
		`{"type":"task.result","status":"success"}`,
		`{"type":"artifact.chunk","sequence":0}`,
		`{"type":"artifact.complete","chunks":1}`,
	}
	target.Handle(func(stream rpcclient.ServerStream) error {
		data, err := stream.Recv()
		if err != nil {
			return err
		}
		if string(data) != dispatcherRoutingRequestPayload {
			t.Fatalf("stream.Recv() = %q, want request", string(data))
		}

		go func() {
			_, recvErr := stream.Recv()
			recvDone <- recvErr
		}()

		for _, response := range responses {
			if err := stream.Send([]byte(response)); err != nil {
				return err
			}
			time.Sleep(75 * time.Millisecond)
		}
		return nil
	})

	sourceCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", "planner-source-many-blocked-e2ee",
		"x-auth-method", "apikey",
	))
	sourceTunnel, err := client.AgentTunnel(sourceCtx)
	if err != nil {
		t.Fatalf("source AgentTunnel: %v", err)
	}
	source := rpcclient.NewAgent("planner-source-many-blocked-e2ee", sourceTunnel)
	source.EnableA2AE2EE()
	defer source.Close() //nolint:errcheck

	stream, err := source.NewStream(ctx, "worker-target-many-blocked-e2ee")
	if err != nil {
		t.Fatalf("NewStream() error = %v", err)
	}
	if err := stream.Send([]byte(dispatcherRoutingRequestPayload)); err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	for index, want := range responses {
		got, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv(%d) error = %v", index, err)
		}
		if string(got.Data) != want {
			t.Fatalf("Recv(%d) = %q, want %q", index, string(got.Data), want)
		}
	}

	select {
	case recvErr := <-recvDone:
		if !errors.Is(recvErr, context.Canceled) && recvErr != io.EOF {
			t.Fatalf("background Recv() error = %v, want context.Canceled or io.EOF", recvErr)
		}
	case <-ctx.Done():
		t.Fatal("background Recv() did not unblock")
	}
}
