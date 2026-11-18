// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"context"
	"errors"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
)

// revokeAfterRead is the key file's provider with a revocation landing
// right after its lookup: between Authenticate's read and the hold.
type revokeAfterRead struct {
	Provider
	km *KeyManager
	id *identity.MachineID
}

func (r revokeAfterRead) Authenticate(ctx context.Context, c identity.Credential) (identity.Principal, error) {
	p, err := r.Provider.Authenticate(ctx, c)
	_ = r.km.Revoke(*r.id)
	return p, err
}

// TestKeyFileRevocationDuringAuthenticate: a key revoked (KeyManager.Revoke
// -> OnRevoke -> Authenticator.Revoke) while its connection authenticates
// is cut at Hold and not cached (relay-server spec/README.md finding 5).
func TestKeyFileRevocationDuringAuthenticate(t *testing.T) {
	km, pr, ca := certManager(t)
	m := enrol(t, km, ca, "agent-1", "")
	a := identity.NewAuthenticator(ProviderName, revokeAfterRead{pr, km, &m.ID})
	km.OnRevoke(func(id string) { a.Revoke(identity.Key(ProviderName, id)) })

	p, err := a.Authenticate(t.Context(), identity.Credential{PeerCerts: m.chain})
	if err != nil {
		t.Fatal(err)
	}
	held := a.Hold(t.Context(), p)
	defer held.Release()
	if !errors.Is(context.Cause(held), identity.ErrRevoked) {
		t.Errorf("held after the revocation: cause %v", context.Cause(held))
	}
	if p, err := a.Authenticate(t.Context(), identity.Credential{PeerCerts: m.chain}); err == nil {
		t.Errorf("the revoked key authenticated again (a cached answer): %+v", p)
	}
}
