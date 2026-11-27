// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// The agent wire's hard cut to package nfltr.relay.v1 (client generation
// 4): a call of an older client to a service this relay does not serve (the
// agent wire under its earlier package) is refused with the update
// message, which released clients stop on; a current client's call to an
// unknown service is gRPC's Unimplemented.
func TestUnknownServiceRefusesOldGeneration(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := NewGRPCServer(slog.New(slog.NewTextHandler(io.Discard, nil)))
	pb.RegisterDispatcherServer(srv, pb.UnimplementedDispatcherServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	call := func(gen int) error {
		md := append(agentwire.Self().HelloPairs(), agentwire.GenerationKey, strconv.Itoa(gen))
		stream, err := conn.NewStream(metadata.AppendToOutgoingContext(ctx, md...),
			&grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/legacy.Dispatcher/AgentTunnel")
		if err != nil {
			return err
		}
		return stream.RecvMsg(&pb.TunnelFrame{})
	}
	err = call(agentwire.Generation - 1)
	if st := status.Convert(err); st.Code() != codes.FailedPrecondition || st.Message() != agentwire.ClientGenerationTooOld {
		t.Fatalf("older client: %v; want FailedPrecondition %q", err, agentwire.ClientGenerationTooOld)
	}
	if err := call(agentwire.Generation); status.Code(err) != codes.Unimplemented || status.Convert(err).Message() != "unknown service legacy.Dispatcher" {
		t.Fatalf("current client: %v; want Unimplemented", err)
	}
}
