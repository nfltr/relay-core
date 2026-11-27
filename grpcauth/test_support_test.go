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

// tokens is a token provider of the key file's name: each token is one
// machine's (the key file itself authenticates certificates only).
type tokens map[string]keyfile.Machine

func (tokens) Name() string { return keyfile.ProviderName }

func (t tokens) Authenticate(_ context.Context, c identity.Credential) (identity.Principal, error) {
	m, ok := t[c.Token]
	if !ok || c.Token == "" {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	return identity.Principal{Owner: string(m.AccountOf()), Machine: m.ID, Subject: string(m.ID), CacheTTL: keyfile.CredentialCacheTTL}, nil
}

// authFor is keys as the relay's provider chain (keys may be nil).
func authFor(keys tokens) *identity.Authenticator {
	var provs []identity.Provider
	if keys != nil {
		provs = append(provs, keys)
	}
	return identity.NewAuthenticator("accounts", provs...)
}

func validateTokenFromMetadata(ctx context.Context, keys tokens) (context.Context, error) {
	p, err := metadataPrincipal(ctx, authFor(keys))
	if err != nil || p.Subject == "" {
		return ctx, err
	}
	return identity.WithPrincipal(ctx, p), nil
}

func TokenUnaryInterceptor(keys tokens) grpc.UnaryServerInterceptor {
	return AuthUnaryInterceptor(authFor(keys))
}

func TokenStreamInterceptor(keys tokens) grpc.StreamServerInterceptor {
	return AuthStreamInterceptor(authFor(keys))
}
