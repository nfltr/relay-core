// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"crypto/sha256"
	"encoding/base32"

	"github.com/nfltr/relay-core/keyfile"

	"github.com/nfltr/relay-core/core/identity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// testPrincipal stands in for the key interceptor in tests that are not
// about authentication: the identity a test names in metadata
// ("x-apikey-agentID", "x-apikey-base-id") becomes the principal the
// interceptor would put into the context. Production never reads them.
func testPrincipal(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	id := FirstMD(md, "x-apikey-agentid")
	if id == "" {
		return ctx
	}
	base := FirstMD(md, "x-apikey-base-id")
	owner := base
	if owner == "" {
		owner = id
	}
	p := identity.Principal{Provider: keyfile.ProviderName, Owner: "keys:" + owner, Subject: base, AgentID: id}
	if ref, err := identity.ParseAgentRef(id); err == nil {
		p.Machine = ref.Machine
	}
	return identity.WithPrincipal(ctx, p)
}

// testMachine is a machine id fixed by seed, so a test names the same
// machine twice by one word.
func testMachine(seed string) identity.MachineID {
	sum := sha256.Sum256([]byte(seed))
	return identity.MachineID("m-" + base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding).EncodeToString(sum[:16]))
}

// testRef is service svc's agent ref on testMachine(seed).
func testRef(seed, svc string) string {
	return identity.AgentRef{Machine: testMachine(seed), Service: svc}.String()
}

// testPrincipalStream and testPrincipalUnary are testPrincipal as server
// interceptors, for test servers that serve clients sending that metadata.
func testPrincipalStream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	return h(srv, &principalStream{ServerStream: ss, ctx: testPrincipal(ss.Context())})
}

func testPrincipalUnary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	return h(testPrincipal(ctx), req)
}

type principalStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *principalStream) Context() context.Context { return s.ctx }
