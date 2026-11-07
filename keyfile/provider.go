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
// database): a machine's owner is its account (Machine.AccountOf: its own
// id, or the account it joined at generate), so the machines of one
// account reach each other's agents (spec/KeyAccount.tla
// SameAccountReaches); it registers any valid service name (its own id's
// refs) and lives until it is revoked (KeyManager.Revoke, which drops the
// core's cached answers). It links no database.
type Provider struct{ Store *APIKeyStore }

// Name implements identity.Provider.
func (Provider) Name() string { return ProviderName }

// Authenticate implements identity.Provider: one map lookup.
func (k Provider) Authenticate(_ context.Context, c identity.Credential) (identity.Principal, error) {
	if k.Store == nil {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	m, ok := k.Store.Validate(c.Token)
	if !ok {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	id := string(m.ID)
	return identity.Principal{Owner: string(m.AccountOf()), Machine: m.ID, Subject: id, CacheTTL: CredentialCacheTTL}, nil
}

// Policy is the reach policy of a key-file relay: an account is one group
// (relay-core Reach checked the account first), and a machine name
// resolves within the caller's account only (session.NameResolver; spec
// KeyAccount.tla ResolveOwnAccount), read from the live store, which a
// reload swaps whole.
type Policy struct{ Store *APIKeyStore }

// Allowed implements session.Policy: every machine of an account reaches
// every agent of it.
func (Policy) Allowed(identity.Principal, identity.AgentRef, identity.Action) (bool, error) {
	return true, nil
}

// ResolveMachine implements session.NameResolver: the machine of caller's
// account named name, "" for none (a caller of another provider has no
// key-file account).
func (k Policy) ResolveMachine(caller identity.Principal, name string) (identity.MachineID, error) {
	if k.Store == nil || caller.Machine == "" {
		return "", nil
	}
	return k.Store.resolve(caller.Machine, name), nil
}

// resolve is the machine named name in the account of machine caller.
// ponytail: one scan of the file's machines per name; an (account, name)
// index swapped with the keys if key files grow to thousands of lines.
func (s *APIKeyStore) resolve(caller identity.MachineID, name string) identity.MachineID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var account identity.MachineID
	for _, m := range s.keys {
		if m.ID == caller {
			account = m.AccountOf()
		}
	}
	if account == "" {
		return ""
	}
	for _, m := range s.keys {
		if m.Name == name && m.AccountOf() == account {
			return m.ID
		}
	}
	return ""
}
