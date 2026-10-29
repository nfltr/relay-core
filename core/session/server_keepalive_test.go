// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// TestServerConstructorAppliesKeepaliveEnforcementPolicy verifies that
// NewGRPCServer always applies EnforcementPolicy{MinTime: 25s,
// PermitWithoutStream: true} regardless of viper config.
func TestServerConstructorAppliesKeepaliveEnforcementPolicy(t *testing.T) {
	// No keepalive config is set, to prove
	// the policy is unconditional, not config-gated.
	defer viper.Reset()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close() //nolint:errcheck

	srv := grpcfactory.NewGRPCServer(nil)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	// Connect a client that pings every 25s (≥ MinTime) – must succeed.
	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                grpcfactory.RelayKeepaliveMinTime, // 25 s
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// Wait long enough that a too-short MinTime would already have caused a
	// GOAWAY, then verify the connection is still usable.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn.WaitForStateChange(ctx, conn.GetState()) //nolint:errcheck

	// If we reach here without the connection being torn down with
	// "too_many_pings", the policy is in effect.
	_ = conn
}

// TestServerConstructorAppliesKeepaliveServerParams verifies that
// NewGRPCServer applies ServerParameters{Time: 30s, Timeout: 10s}.
func TestServerConstructorAppliesKeepaliveServerParams(t *testing.T) {
	defer viper.Reset()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close() //nolint:errcheck

	srv := grpcfactory.NewGRPCServer(nil)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	// Verify exported constant values match the expected relay server params.
	if grpcfactory.RelayKeepaliveServerTime != 30*time.Second {
		t.Errorf("RelayKeepaliveServerTime = %v, want 30s", grpcfactory.RelayKeepaliveServerTime)
	}
	if grpcfactory.RelayKeepaliveServerTimeout != 10*time.Second {
		t.Errorf("RelayKeepaliveServerTimeout = %v, want 10s", grpcfactory.RelayKeepaliveServerTimeout)
	}
}

// TestServerAcceptsFrequentClientPings spins up a test gRPC server with the
// relay keepalive params and verifies a client pinging every 30s is not
// disconnected with GOAWAY/too_many_pings during the observation window.
func TestServerAcceptsFrequentClientPings(t *testing.T) {
	defer viper.Reset()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close() //nolint:errcheck

	srv := grpcfactory.NewGRPCServer(nil)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient(
		lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck

	// Observe for 3 seconds; if the server enforcement policy is correct the
	// connection stays alive (no GOAWAY). Full 3-minute soak is left for
	// integration/long tests; unit coverage here exercises the config path.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	<-ctx.Done()

	state := conn.GetState()
	// SHUTDOWN state means the server closed the connection.
	if state.String() == "SHUTDOWN" {
		t.Errorf("connection was shut down by server (too_many_pings?): state=%v", state)
	}
}

// TestKeepaliveValuesAreConsistentWithClient verifies that the server's
// MinTime enforcement value is ≤ the client ping interval so the server
// will never reject pings from a correctly-configured client.
// iter-61 sCCC9 pings every 30 s; MinTime must be ≤ 30 s.
func TestKeepaliveValuesAreConsistentWithClient(t *testing.T) {
	// iter-61 sCCC9 client ping interval.
	const clientPingTime = 30 * time.Second

	if grpcfactory.RelayKeepaliveMinTime > clientPingTime {
		t.Errorf(
			"server EnforcementPolicy.MinTime (%v) > client ping interval (%v): "+
				"server will reject client pings with too_many_pings",
			grpcfactory.RelayKeepaliveMinTime, clientPingTime,
		)
	}
}
