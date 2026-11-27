// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"crypto/x509"
	"time"

	"github.com/nfltr/relay-core/core/identity"
)

// ProviderName is the key file's provider name: the namespace its owners
// live in (identity.Principal).
const ProviderName = "keys"

// CredentialCacheTTL is how long the core keeps a certificate's answer
// (identity.Principal.CacheTTL); a revoke drops it at once.
const CredentialCacheTTL = 30 * time.Second

// Provider is the key file as an identity provider (a relay with no
// database): it authenticates a TLS client certificate of the data dir's
// machine CA (CAs; machine_cert.go: spec/KeyAccount.tla Authenticates),
// never a bearer token. A machine's owner is its account
// (Machine.AccountOf: its own id, or the account it joined at enrolment),
// so the machines of one account reach each other's agents
// (spec/KeyAccount.tla SameAccountReaches); it registers any valid service
// name (its own id's refs) and lives until it is revoked
// (KeyManager.Revoke, which drops the core's cached answers) or its
// certificate ends. It links no database.
type Provider struct {
	Store *Store
	CAs   *x509.CertPool
}

// Name implements identity.Provider.
func (Provider) Name() string { return ProviderName }

// Authenticate implements identity.Provider: a certificate chain's
// verification and one map lookup; any other credential is unknown.
func (k Provider) Authenticate(_ context.Context, c identity.Credential) (identity.Principal, error) {
	if k.Store == nil || k.CAs == nil || c.Token != "" || len(c.PeerCerts) == 0 {
		return identity.Principal{}, identity.ErrUnknownCredential
	}
	m, err := k.Store.MachineByCert(k.CAs, c.PeerCerts, time.Now())
	if err != nil {
		return identity.Principal{}, err
	}
	p := principalOf(m)
	p.NotAfter = c.PeerCerts[0].NotAfter
	return p, nil
}

func principalOf(m Machine) identity.Principal {
	return identity.Principal{Owner: string(m.AccountOf()), Machine: m.ID, Subject: string(m.ID), CacheTTL: CredentialCacheTTL}
}

// Policy is the reach policy of a key-file relay: an account is one group
// (relay-core Reach checked the account first), and a machine name
// resolves within the caller's account only (session.NameResolver; spec
// KeyAccount.tla ResolveOwnAccount), read from the live store, which a
// reload swaps whole.
type Policy struct{ Store *Store }

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
// index swapped with the lines if key files grow to thousands of lines.
func (s *Store) resolve(caller identity.MachineID, name string) identity.MachineID {
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
