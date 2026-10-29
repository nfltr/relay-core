// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcauth

import (
	"context"

	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/keyfile"
	"google.golang.org/grpc"
)

// AgentIDFromContext returns the AgentID of the principal the auth
// interceptors put into ctx.
func AgentIDFromContext(ctx context.Context) (string, bool) {
	p, ok := identity.FromContext(ctx)
	return p.AgentID, ok && p.AgentID != ""
}

// authFor is the key file as the relay's provider chain (keys may be nil).
func authFor(keys *keyfile.APIKeyStore) *identity.Authenticator {
	var provs []identity.Provider
	if keys != nil {
		provs = append(provs, keyfile.Provider{Store: keys})
	}
	return identity.NewAuthenticator("accounts", provs...)
}

func validateAPIKeyFromMetadata(ctx context.Context, keys *keyfile.APIKeyStore) (context.Context, error) {
	p, err := MetadataPrincipal(ctx, authFor(keys))
	if err != nil || p.Subject == "" {
		return ctx, err
	}
	return identity.WithPrincipal(ctx, p), nil
}

func APIKeyUnaryInterceptor(keys *keyfile.APIKeyStore) grpc.UnaryServerInterceptor {
	return AuthUnaryInterceptor(authFor(keys))
}

func APIKeyStreamInterceptor(keys *keyfile.APIKeyStore) grpc.StreamServerInterceptor {
	return AuthStreamInterceptor(authFor(keys))
}
