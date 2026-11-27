// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// ---------------------------------------------------------------------------
// NewGRPCServer tests
// ---------------------------------------------------------------------------

func TestNewGRPCServer_Default(t *testing.T) {
	defer viper.Reset()

	s := NewGRPCServer(testLogger())
	if s == nil {
		t.Fatal("expected non-nil gRPC server")
	}
	s.Stop()
}

func TestNewGRPCServer_WithKeepalive(t *testing.T) {
	viper.Set("grpc.keepalive.enforcement_policy.min_time", 5*time.Second)
	viper.Set("grpc.keepalive.enforcement_policy.permit_without_stream", true)
	viper.Set("grpc.keepalive.server_parameters.time", 30*time.Second)
	viper.Set("grpc.keepalive.server_parameters.timeout", 10*time.Second)
	defer viper.Reset()

	s := NewGRPCServer(testLogger())
	if s == nil {
		t.Fatal("expected non-nil gRPC server with keepalive")
	}
	s.Stop()
}
