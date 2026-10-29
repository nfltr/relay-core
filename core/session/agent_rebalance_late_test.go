// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestConnectWhileTheMovedSessionStillEndsIsUnavailable: once a replica
// stops routing to an agent's moved session it still holds that session
// (its settle, then MOVE_DONE) and its subscription; a connection of the
// same agent landing there in that window is refused as Unavailable, which
// clients try again, not with the broker's "subscription already exists"
// (TestRebalanceAfterScaleUp, Linux runner at 30cad49: a second move landed
// on the replica the agent had just left).
func TestConnectWhileTheMovedSessionStillEndsIsUnavailable(t *testing.T) {
	f := newMoverFleet(t, 2)
	a, b := f.replicas[0], f.replicas[1]
	f.route(a)
	f.connect("agent-x")
	f.route(b)
	if !a.moveAgent("agent-x") {
		t.Fatal("agent-x was not asked to move")
	}
	waitMove(t, "a stops routing to the moved session", func() bool {
		_, here := a.Agents.Load("agent-x")
		_, there := b.Agents.Load("agent-x")
		return !here && there
	})
	md := metadata.Pairs("x-apikey-agentID", "agent-x", "x-auth-method", "apikey", agentwire.MoveKey, "1")
	c := &moverConn{ctx: testPrincipal(metadata.NewIncomingContext(context.Background(), md)), accepted: make(chan struct{}),
		steps: make(chan pb.Move, 4), recv: make(chan *pb.HTTPProxyResponse, 4), hungUp: make(chan struct{}), ended: make(chan error, 1)}
	go func() { c.ended <- a.ConnectHTTP(c) }()
	select {
	case <-c.accepted:
		c.hangUp()
		t.Fatal("a admitted agent-x while its moved session there still ends")
	case err := <-c.ended:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("connect while the moved session ends = %v, want Unavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connect neither accepted nor refused")
	}
}
