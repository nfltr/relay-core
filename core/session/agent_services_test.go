// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestDeclaredServicesFailClosed(t *testing.T) {
	ctx := func(vals ...string) context.Context {
		md := metadata.MD{}
		for _, v := range vals {
			md.Append(agentservices.Header, v)
		}
		return metadata.NewIncomingContext(context.Background(), md)
	}
	if d, err := declaredServices(ctx()); d != nil || err != nil {
		t.Fatalf("no declaration: %v %v", d, err)
	}
	if d, err := declaredServices(ctx("web=http,db=tcp")); err != nil || len(d) != 2 {
		t.Fatalf("declaration: %v %v", d, err)
	}
	for _, vals := range [][]string{{"web=http", "db=tcp"}, {"web"}, {"localhost:22=tcp"}, {""}} {
		if _, err := declaredServices(ctx(vals...)); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%q: %v, want InvalidArgument", vals, err)
		}
	}
}

// slowClaimInventory delays the session's inventory claim, a step between
// the accept and the agent's registration that a shared store (Redis)
// makes slow.
type slowClaimInventory struct{ directory.IPInventory }

func (s slowClaimInventory) SetIPWithAgentInfo(ctx context.Context, agentID string, labels map[string]string, capabilities []string, ttl time.Duration) error {
	time.Sleep(200 * time.Millisecond)
	return s.IPInventory.SetIPWithAgentInfo(ctx, agentID, labels, capabilities, ttl)
}

// TestConnectHTTPAcceptsOnceRegistered: an agent learns it is connected
// (ConnectHTTP's response header) only once the relay routes to it, so a
// share it requests for a declared service at once is never refused as
// "not connected here".
func TestConnectHTTPAcceptsOnceRegistered(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	d.IPInventory = slowClaimInventory{d.IPInventory}
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "svc-agent"))
	ctx = metadata.AppendToOutgoingContext(ctx, agentservices.Header, "web=http")
	stream, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Header(); err != nil {
		t.Fatal(err)
	}
	if services, connected := d.AgentServices("svc-agent"); !connected || len(services) != 1 {
		t.Fatalf("accepted but not registered: services %v, connected %v", services, connected)
	}

	// The same for an AgentTunnel (A2A) agent.
	d.StreamerIPInventory = slowClaimInventory{d.StreamerIPInventory}
	a2a, err := client.AgentTunnel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a2a.Header(); err != nil {
		t.Fatal(err)
	}
	if _, ok := d.Upstreams.Load("svc-agent"); !ok {
		t.Fatal("AgentTunnel accepted but not registered")
	}
}
