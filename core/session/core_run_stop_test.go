// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"google.golang.org/grpc"
)

// A stop that wins the race with serving (the context ended before the
// port was served: grpc.ErrServerStopped) is a clean stop, as a stop
// during serving is (ExampleServe in compose flaked on it under load).
func TestServeAgentsStopBeforeServe(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	stopped := make(chan struct{})
	serve := func(l net.Listener) error {
		<-stopped // serve only after the stop
		return s.Serve(l)
	}
	stopper := stopNotifier{s, stopped}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := NewDispatcher(context.Background(), slog.New(slog.DiscardHandler), nil, nil,
		directory.NewNoOpInventory(), directory.NewNoOpInventory(), time.Second)
	c := &Core{Dispatcher: d}
	err = c.ServeAgents(ctx, slog.New(slog.DiscardHandler), stopper, serve, l, nil, time.Second,
		func(time.Duration) time.Duration { return 0 })
	if err != nil {
		t.Fatalf("ServeAgents stopped before serving: %v", err)
	}
}

type stopNotifier struct {
	*grpc.Server
	stopped chan struct{}
}

func (s stopNotifier) GracefulStop() { s.Server.GracefulStop(); close(s.stopped) }
