// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"os"
	"strings"
	"testing"
)

// TestRelayServerUsesLargeMessageCodec: the relay's gRPC server keeps its
// 8 MiB body chunks out of grpc-go's shared buffer pool (idle 32 MiB of the
// relay's heap during a share, measured). grpc.Server exposes no codec
// getter, so this pins the option in NewGRPCServer.
func TestRelayServerUsesLargeMessageCodec(t *testing.T) {
	src, err := os.ReadFile("grpc_server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "grpc.ForceServerCodecV2(grpccodec.NewLargeMessage())") {
		t.Fatal("NewGRPCServer no longer sets grpc.ForceServerCodecV2(grpccodec.NewLargeMessage())")
	}
}
