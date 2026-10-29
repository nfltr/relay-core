// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/internal/platform/leakcheck"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay/client/grpcclient"
	pb "github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const testKey = "alice:rpc_lr11testkeyaaaaaaaaaaaaaaaaaaaaaaaa"

func TestMain(m *testing.M) {
	if os.Getenv(childEnv) == "1" { // a test runs this binary as nfltr-relay-core (core_e2e_test.go)
		// The process's own stderr: built with -tags linkdebug, leakcheck's
		// init tees os.Stderr through a pipe, and a child exiting at once
		// (a refused start) would exit before the tee wrote its last lines.
		os.Stderr = os.NewFile(2, "/dev/stderr")
		os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
	}
	args := os.Args
	os.Args = os.Args[:1] // config parses the process argv; the test flags are testing's
	_ = os.Setenv("NFLTR_API_KEY", testKey)
	_ = os.Setenv("TCP_ENABLED", "true")
	config.SetDefaultFlags()
	os.Args = args
	os.Exit(leakcheck.Run(m))
}

// TestCoreServesAgents: the core alone (no RPCServer, no database) answers
// /ready, admits an agent with a key from the key file (its TCP data
// plane bound) and refuses an unknown key as Unauthenticated.
func TestCoreServesAgents(t *testing.T) {
	var ls compose.Ports
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal, &ls.Metrics} {
		var err error
		if *l, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- compose.Serve(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), ls, frontends) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	res, err := http.Get("http://" + ls.Internal.Addr().String() + "/ready")
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("/ready = %d", res.StatusCode)
	}

	conn, err := grpc.NewClient(ls.GRPC.Addr().String(), append(grpcclient.ClientIdentity(), grpc.WithTransportCredentials(insecure.NewCredentials()))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := pb.NewDispatcherClient(conn)
	keyed := func(key string) context.Context {
		return metadata.NewOutgoingContext(t.Context(), metadata.Pairs("x-api-key", key, "x-agent-id", "web"))
	}

	agent, err := client.ConnectTCP(keyed(testKey[len("alice:"):]))
	if err != nil {
		t.Fatal(err)
	}
	if md, err := agent.Header(); err != nil || len(md.Get("x-nfltr-tcp-bind")) != 1 {
		t.Fatalf("agent header %v, %v: want its data plane bound", md, err)
	}

	stranger, err := client.ConnectTCP(keyed("rpc_unknownkeyaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	if err == nil {
		_, err = stranger.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("an unknown key: %v, want Unauthenticated", err)
	}
}
