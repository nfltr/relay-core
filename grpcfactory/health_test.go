// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"context"
	"net"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay/client/grpcclient"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

// The gRPC port answers the standard health check without a credential, so
// a client can tell it from the HTTPS port (both TLS, both h2): nfltr
// status --server <the HTTPS port> said online (nfltr
// docs/reports/cli-ux-2026-11-20.md finding 8).
func TestHealthAnswersWithoutCredential(t *testing.T) {
	defer viper.Reset()
	s := NewGRPCServer(testLogger(), WithAuthenticator(identity.NewAuthenticator("accounts", keyfile.Provider{Store: keyfile.NewStore()})))
	lis := bufconn.Listen(1 << 20)
	go func() { _ = s.Serve(lis) }()
	defer s.Stop()

	opts := append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}, grpcclient.ClientIdentity()...)
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close() //nolint:errcheck
	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health = %v, want SERVING", resp.GetStatus())
	}
}
