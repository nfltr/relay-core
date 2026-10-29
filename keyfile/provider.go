// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// ProviderName is the key file's provider name: the namespace its owners
// live in (identity.Principal).
const ProviderName = "keys"

// CredentialCacheTTL is how long the core keeps a key's answer
// (identity.Principal.CacheTTL); a revoke drops it at once.
const CredentialCacheTTL = 30 * time.Second

// Provider is the static key file as an identity provider (a relay with no
// database): every key owns itself, may take any agent name under itself,
// and lives until it is revoked (KeyManager.Revoke, which drops the
// core's cached answers). It links no database.
type Provider struct{ Store *APIKeyStore }

// Name implements identity.Provider.
func (Provider) Name() string { return ProviderName }

// MintsSubjects implements identity.Minting: a key's id is its agent id.
func (Provider) MintsSubjects() {}

// Authenticate implements identity.Provider: one map lookup.
func (k Provider) Authenticate(_ context.Context, c identity.Credential) (identity.Principal, error) {
	if k.Store == nil {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	id, ok := k.Store.Validate(c.Token)
	if !ok {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	return identity.Principal{Owner: id, Subject: id, Names: identity.AnyName, CacheTTL: CredentialCacheTTL}, nil
}
