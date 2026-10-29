// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"testing"

	"google.golang.org/grpc/metadata"
)

// TestWithoutClientAssertedIdentityCopiesOnlyToStrip: a stream whose client
// sent no server-asserted key keeps its metadata as is. A copy per stream
// was about 1 KiB held for each idle agent stream (five per E2EE agent).
func TestWithoutClientAssertedIdentityCopiesOnlyToStrip(t *testing.T) {
	clean := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "k", "x-agent-id", "a"))
	if withoutClientAssertedIdentity(clean) != clean {
		t.Fatal("metadata without asserted keys was copied")
	}
	forged := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "k", "x-auth-method", "apikey", "x-apikey-agentid", "victim"))
	md, _ := metadata.FromIncomingContext(withoutClientAssertedIdentity(forged))
	if len(md.Get("x-auth-method")) != 0 || len(md.Get("x-apikey-agentid")) != 0 || len(md.Get("x-api-key")) != 1 {
		t.Fatalf("stripped metadata = %v, want the asserted keys removed and the rest kept", md)
	}
	if orig, _ := metadata.FromIncomingContext(forged); len(orig.Get("x-auth-method")) != 1 {
		t.Fatal("stripping changed the caller's metadata")
	}
}
