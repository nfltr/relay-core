// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"net"
	"testing"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Relay tunnels are the replica link's: the Dispatcher serves no
// RelayTunnel RPC to clients (a caller with a valid key gets Unimplemented),
// and a relay tunnel that does not come from a replica link peer is
// refused before any routing.
func TestRelayTunnelIsNotClientCallable(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterDispatcherServer(srv, d)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	ctx := metadata.AppendToOutgoingContext(t.Context(), "x-api-key", "rpc_key")
	st, err := conn.NewStream(ctx, &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}, "/rpcserver.Dispatcher/RelayTunnel")
	if err == nil {
		_ = st.SendMsg(&pb.TunnelFrame{Routing: &pb.RoutingInfo{AgentId: "bob.laptop"}})
		err = st.RecvMsg(&pb.TunnelFrame{})
	}
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("RelayTunnel on the gRPC port = %v; want Unimplemented", err)
	}

	frame := &pb.TunnelFrame{Routing: &pb.RoutingInfo{AgentId: "bob.laptop"}, Eom: true}
	for name, ctx := range map[string]context.Context{"no credential": context.Background(), "agent key": makeGRPCContext("alice")} {
		if err := d.RelayTunnel(&mockRelayTunnelServer{ctx: ctx, notPeer: true, recvMsgs: []*pb.TunnelFrame{frame}}); status.Code(err) != codes.PermissionDenied {
			t.Errorf("%s: relayTunnel = %v; want PermissionDenied", name, err)
		}
	}
}
