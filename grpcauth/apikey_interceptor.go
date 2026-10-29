// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package security — the gRPC interceptors that authenticate a credential.
//
// A call or stream's credential is "x-api-key", else "authorization:
// Bearer <token>", else the TLS client certificate of its connection
// (TLSStateCredentials); the relay core's provider chain
// (identity.Authenticator: the key file, the accounts store, the
// configured providers such as mTLS and OIDC) authenticates it, and its
// principal goes into the context (identity.WithPrincipal), typed, never
// as strings in metadata. An x-api-key no provider accepts is refused
// (Unauthenticated); a bearer token or a certificate no provider knows is
// as if absent (a gateway call may carry another service's bearer), one a
// provider refuses is refused. A call without a credential passes on
// (trust-loopback); the handlers refuse it if nothing authenticated it. A
// stream is held under its credential for as long as it runs: revoking the
// credential, or its expiry, ends it.
package grpcauth

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/nfltr/relay-core/core/identity"
	relayv1 "github.com/nfltr/relay/proto/nfltr/relay/v1"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthUnaryInterceptor authenticates a call's key with auth.
func AuthUnaryInterceptor(auth *identity.Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		p, err := MetadataPrincipal(ctx, auth)
		if err != nil {
			return nil, err
		}
		if p.Subject != "" {
			ctx = identity.WithPrincipal(ctx, p)
		}
		return handler(ctx, req)
	}
}

// AuthStreamInterceptor authenticates a stream's key with auth and holds
// the stream under the credential until it ends. A stream its credential's
// revocation or expiry ended answers Unauthenticated, reason
// ERROR_REASON_UNAUTHENTICATED.
func AuthStreamInterceptor(auth *identity.Authenticator) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		p, err := MetadataPrincipal(ss.Context(), auth)
		if err != nil {
			return err
		}
		if p.Subject == "" {
			return handler(srv, ss)
		}
		held := auth.Hold(ss.Context(), p) // carries p
		defer held.Release()
		err = handler(srv, &wrappedStream{ServerStream: ss, ctx: held})
		if cause := context.Cause(held); errors.Is(cause, identity.ErrRevoked) || errors.Is(cause, identity.ErrExpired) {
			return CredentialEndedStatus(cause).Err()
		}
		return err
	}
}

// MetadataPrincipal authenticates the incoming credential (with the
// "x-agent-id" name and labels it asks for). No credential: a zero
// principal, no error.
func MetadataPrincipal(ctx context.Context, auth *identity.Authenticator) (identity.Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || auth == nil {
		return identity.Principal{}, nil
	}
	c := identity.Credential{Token: first(md, "x-api-key"), Name: first(md, "x-agent-id")}
	optional := c.Token == "" // a bearer token or a certificate
	if optional {
		if tok, ok := strings.CutPrefix(first(md, "authorization"), "Bearer "); ok {
			c.Token = tok
		} else if c.PeerCerts = PeerCertificates(ctx); c.PeerCerts == nil {
			return identity.Principal{}, nil
		}
	}
	if labels := first(md, agentwire.LabelsKey); labels != "" {
		c.Labels = agentwire.ParseLabels(labels)
	}
	p, err := auth.Authenticate(ctx, c)
	if optional && errors.Is(err, identity.ErrUnknownCredential) {
		return identity.Principal{}, nil
	}
	if err != nil {
		slog.Warn("security: credential refused on gRPC", "error", err)
		if errors.Is(err, identity.ErrNameNotAllowed) {
			return p, status.Error(codes.PermissionDenied, err.Error())
		}
		if errors.Is(err, identity.ErrUnavailable) {
			return p, status.Error(codes.Unavailable, "credential store unavailable, retry")
		}
		return p, status.Error(codes.Unauthenticated, "invalid API key or fleet token")
	}
	return p, nil
}

func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

// wrappedStream overrides Context() to return the enriched context.
type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }

// CredentialEndedStatus is the typed answer of a connection its
// credential's revocation or expiry ended.
func CredentialEndedStatus(cause error) *status.Status {
	st := status.New(codes.Unauthenticated, cause.Error())
	if d, err := st.WithDetails(&errdetails.ErrorInfo{Reason: relayv1.ErrorReason_ERROR_REASON_UNAUTHENTICATED.String(), Domain: "relay.nfltr.xyz"}); err == nil {
		return d
	}
	return st
}
