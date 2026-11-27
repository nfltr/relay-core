// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"
	"errors"
	"net"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/nf1anon"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentservices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// NF1AnonStreamInterceptor registers an agent stream that no credential
// authenticated (it runs after AuthStreamInterceptor) and that declares
// only capability addresses, each with its registration proof, as
// anonymous (core/nf1anon; nfltr docs/decisions/nf1-anon-2026-11-16.md
// section 1), when r takes such registrations: its ConnectHTTP counts
// against the anonymous caps, its other streams join that registration.
// Each is held under the anonymous principal until its NotAfter, when it
// ends Unavailable (the agent connects again: a new registration). Any
// other stream passes on as it came.
func NF1AnonStreamInterceptor(auth *identity.Authenticator, r *nf1anon.Registry) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()
		if _, ok := identity.FromContext(ctx); ok || auth == nil || !r.Enabled() {
			return handler(srv, ss)
		}
		var ip net.IP
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
				ip = net.ParseIP(host)
			}
		}
		register := info.FullMethod == pb.Dispatcher_ConnectHTTP_FullMethodName
		p, release, err := r.Register(firstValue(ctx, agentservices.Header), metadata.ValueFromIncomingContext(ctx, agentservices.RegHeader), ip, register)
		if errors.Is(err, nf1anon.ErrNotAnonymous) {
			return handler(srv, ss)
		}
		if err != nil {
			return err
		}
		defer release()
		held := auth.Hold(ctx, p)
		defer held.Release()
		err = handler(srv, &wrappedStream{ServerStream: ss, ctx: held})
		if cause := context.Cause(held); errors.Is(cause, identity.ErrExpired) {
			if register {
				r.Expired(p.AgentID)
			}
			return status.Error(codes.Unavailable, "nf1: the registration with no account reached its lifetime: connect again")
		}
		return err
	}
}

// firstValue is the first value of the incoming metadata key, as first.
func firstValue(ctx context.Context, key string) string {
	if v := metadata.ValueFromIncomingContext(ctx, key); len(v) > 0 {
		return v[0]
	}
	return ""
}
