// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"testing"

	"github.com/nfltr/relay-core/core/identity"
)

// TestStreamCredentialOfACertificate: a stream with no token (a machine
// certificate) is tagged by its principal's machine, so the same machine's
// next session takes its agent over and another machine's does not.
func TestStreamCredentialOfACertificate(t *testing.T) {
	of := func(m identity.MachineID) string {
		return streamCredential(identity.WithPrincipal(context.Background(), identity.Principal{Provider: "keyfile", Machine: m}))
	}
	a, b := of("m-a"), of("m-b")
	if a == "" || a != of("m-a") || a == b {
		t.Fatalf("credentials a %q (again %q), b %q: want one per machine, non-empty", a, of("m-a"), b)
	}
	if got := streamCredential(context.Background()); got != "" {
		t.Fatalf("no principal, no token: %q", got)
	}
}
