// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"

	"github.com/nfltr/relay-core/core/identity"
)

// PrincipalOwner is the owner of the principal ctx carries ("" none).
func PrincipalOwner(ctx context.Context) string {
	p, _ := identity.FromContext(ctx)
	return p.Owner
}

// keyBaseOf is the credential subject (a key's agent id, a token's id) of
// the principal ctx carries ("" none).
func keyBaseOf(ctx context.Context) string {
	p, _ := identity.FromContext(ctx)
	return p.Subject
}
