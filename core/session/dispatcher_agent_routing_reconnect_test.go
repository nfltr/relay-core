// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay/client/rpcclient"
	"github.com/spf13/viper"
	"google.golang.org/grpc/metadata"
)

func TestAgentTunnel_ReconnectKeepsReplacementUpstreamState(t *testing.T) {
	client, d, cleanup := newA2ABufConnEnv(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const (
		sourceAgentID = "planner-reconnect"
		keyBase       = "test-key"
		targetAgentID = "worker-target-reconnect"
	)

	targetCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
		"x-apikey-agentID", targetAgentID,
		"x-auth-method", "apikey",
	))
	targetTunnel, err := client.AgentTunnel(targetCtx)
	if err != nil {
		t.Fatalf("target AgentTunnel: %v", err)
	}
	target := rpcclient.NewAgent(targetAgentID, targetTunnel)
	awaitUpstreamRegistered(t, d, targetAgentID)
	defer target.Close() //nolint:errcheck

	releaseResult := make(chan struct{})
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
		<-releaseResult
		if err := stream.Send([]byte("result")); err != nil {
			return err
		}
		return nil
	})

	// One planner process reconnecting: both connections carry its
	// instance id (rpcclient.Dial does), so the newer one replaces the
	// older as a reconnect, not a takeover by another process.
	connectPlanner := func() (*rpcclient.Agent, error) {
		plannerCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(
			"x-apikey-agentID", sourceAgentID,
			"x-auth-method", "apikey",
			"x-apikey-base-id", keyBase,
			rpcclient.InstanceHeader, "planner-process",
		))
		tunnel, err := client.AgentTunnel(plannerCtx)
		if err != nil {
			return nil, err
		}
		return rpcclient.NewAgent(sourceAgentID, tunnel), nil
	}

	alpha, err := connectPlanner()
	if err != nil {
		t.Fatalf("alpha AgentTunnel: %v", err)
	}
	defer alpha.Close() //nolint:errcheck
	// AgentTunnel returns before the relay's handler registers the stream:
	// the reconnect must reach the relay after the connection it replaces,
	// as a real one does (it dials only after its old connection died).
	awaitUpstreamRegistered(t, d, sourceAgentID)
	alphaStream, _ := d.Upstreams.Load(sourceAgentID)

	beta, err := connectPlanner()
	if err != nil {
		t.Fatalf("beta AgentTunnel: %v", err)
	}
	defer beta.Close() //nolint:errcheck
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		if cur, ok := d.Upstreams.Load(sourceAgentID); ok && cur != alphaStream {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the reconnect never registered")
		}
	}

	stream, err := beta.NewStream(ctx, targetAgentID)
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

	if err := alpha.Close(); err != nil {
		t.Fatalf("alpha Close() error = %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		current, ok := d.Upstreams.Load(sourceAgentID)
		cachedKeyBase, cached := d.AgentKeyBase.Load(sourceAgentID)
		if ok && current.Context() != nil && current.Context().Err() == nil && cached && cachedKeyBase == keyBase {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replacement planner upstream was not preserved after reconnect: upstream_ok=%t key_base_ok=%t key_base=%q", ok, cached, cachedKeyBase)
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(releaseResult)

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

// ---------------------------------------------------------------------------
// Pod dialer k8s mode gating
// ---------------------------------------------------------------------------

func TestNewDispatcher_PodDialerNilWhenNotK8s(t *testing.T) {
	tests := []struct {
		name          string
		env           string
		wantDialerNil bool
	}{
		{"local env", config.EnvLocal, true},
		{"standalone env", config.EnvStandalone, true},
		{"CI env", config.EnvCI, false},
		{"empty env", "", true},
		{"k8s env", config.EnvK8s, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Set("ENV", tt.env)
			defer viper.Reset()

			d := NewDispatcher(
				context.Background(),
				fake.TestLogger(),
				broker.NewInMemoryMessageBroker(),
				nil,
				nil,
				nil,
				5*time.Second,
			)

			gotNil := d.PodDialer == nil
			if gotNil != tt.wantDialerNil {
				t.Errorf("podDialer nil = %v, want %v (ENV=%q)", gotNil, tt.wantDialerNil, tt.env)
			}
		})
	}
}
