// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// A second ConnectHTTP of an agent ID with the same credential takes the
// agent over and the replaced session is told (FailedPrecondition, which
// clients treat as final); one with another credential is refused
// (AlreadyExists) and the holder stays (spec/Takeover.tla Connect,
// Notice, Stop; its trace is checked by make spec-traces).
func TestConnectHTTPTakeover(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "agent-1"))
	connect := func(token string) (pb.Dispatcher_ConnectHTTPClient, context.CancelFunc) {
		ctx, stop := context.WithCancel(metadata.AppendToOutgoingContext(ctx, "x-nfltr-token", token))
		s, err := client.ConnectHTTP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return s, stop
	}
	accepted := func(s pb.Dispatcher_ConnectHTTPClient) {
		t.Helper()
		if h, err := s.Header(); err != nil || len(h.Get("x-resolved-agent-id")) == 0 {
			t.Fatalf("not accepted: %v %v", h, err)
		}
	}
	first, stopFirst := connect("key-1")
	defer stopFirst()
	accepted(first)
	second, stopSecond := connect("key-1")
	accepted(second)
	if _, err := first.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("the replaced session ended with %v, want the replaced status", err)
	}
	other, stopOther := connect("key-2")
	defer stopOther()
	if _, err := other.Recv(); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("another credential: %v, want AlreadyExists", err)
	}
	if !agentConnected(d, "agent-1") {
		t.Fatal("the refusal ended the holder")
	}
	stopSecond()
	waitUntil(t, func() bool { return !agentConnected(d, "agent-1") }, func() string { return "the stopped session stayed" })
	third, stopThird := connect("key-2") // the agent is free again
	defer stopThird()
	accepted(third)
}
