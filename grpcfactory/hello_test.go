// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"context"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/nfltr/relay/client/grpcclient"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentwire"
)

// The protocol v1 hard cut: a call without a Hello (every client released
// before v1) is refused with one clear, typed answer before anything else
// runs; a v1 client (grpcclient's interceptors) gets the relay's Welcome.
func TestProtocolV1HelloHardCut(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := NewGRPCServer(slog.New(slog.NewTextHandler(io.Discard, nil)))
	pb.RegisterDispatcherServer(srv, pb.UnimplementedDispatcherServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	dial := func(opts ...grpc.DialOption) pb.DispatcherClient {
		conn, err := grpc.NewClient("passthrough://bufnet", append(opts,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return pb.NewDispatcherClient(conn)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A pre-v1 client: its own identity keys, no Hello.
	old := metadata.AppendToOutgoingContext(ctx, "x-nfltr-client", "nfltr", "x-nfltr-client-version", "v1.0.685")
	stream, err := dial().AgentTunnel(old)
	if err == nil {
		_, err = stream.Recv()
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.FailedPrecondition || !strings.Contains(st.Message(), "run `nfltr update`") {
		t.Fatalf("pre-v1 client: %v; want FailedPrecondition telling it to update", err)
	}
	var reason string
	for _, d := range st.Details() {
		if info, ok := d.(*errdetails.ErrorInfo); ok {
			reason = info.GetReason()
		}
	}
	if reason != relayv1.ErrorReason_ERROR_REASON_PROTOCOL_UNSUPPORTED.String() {
		t.Fatalf("reason = %q, want ERROR_REASON_PROTOCOL_UNSUPPORTED", reason)
	}

	// A v1 client: past the hello (the stub answers Unimplemented), with
	// the Welcome in the response headers.
	var header metadata.MD
	stream, err = dial(grpcclient.ClientIdentity()...).AgentTunnel(ctx, grpc.Header(&header))
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("v1 client: %v; want the handler's Unimplemented", err)
	}
	if h, herr := stream.Header(); herr != nil || !agentwire.HasWelcome(h) {
		t.Fatalf("v1 client: no Welcome in %v (%v)", h, herr)
	}
}
