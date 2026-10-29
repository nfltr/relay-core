// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"context"
	"net"
	"testing"

	"github.com/nfltr/relay-core/keyfile"

	"github.com/nfltr/relay-core/core/identity"

	"github.com/nfltr/relay-core/internal/platform/auth"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// A caller with no credential must not become an agent by sending the
// identity metadata the server's own interceptors set after verifying a key.
func TestNewGRPCServer_IgnoresClientAssertedIdentity(t *testing.T) {
	defer viper.Reset()

	keys := keyfile.NewAPIKeyStore()
	keys.Add("owner", "owner-key-supersecret")
	s := NewGRPCServer(testLogger(), WithAuthenticator(identity.NewAuthenticator("accounts", keyfile.Provider{Store: keys})))
	var gotID string
	var gotErr error
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "test.Identity",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Who",
			Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				h := func(ctx context.Context, _ any) (any, error) {
					gotID, gotErr = auth.GetAgentIDFromContext(ctx)
					return new(emptypb.Empty), nil
				}
				return interceptor(ctx, in, &grpc.UnaryServerInfo{FullMethod: "/test.Identity/Who"}, h)
			},
		}},
	}, struct{}{})
	lis := bufconn.Listen(1 << 20)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		append(agentwire.Self().HelloPairs(), "x-apikey-agentid", "victim", "x-mtls-agentid", "victim", "x-auth-method", "apikey")...)
	if err := conn.Invoke(ctx, "/test.Identity/Who", new(emptypb.Empty), new(emptypb.Empty)); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if gotErr == nil {
		t.Fatalf("unauthenticated caller resolved as agent %q", gotID)
	}
}
