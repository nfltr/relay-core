// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build relaytrace

package identity

// revokeTrace is the function TraceRevocations set (nil: none).
type revokeTrace func(ev, key string)

func (t revokeTrace) emit(ev, key string) {
	if t != nil {
		t(ev, key)
	}
}

// TraceRevocations (-tags relaytrace only) has RevokeCause report its two
// steps of relay-server spec/Revocation.tla to f, in order and under the
// lock an Authenticate's cache insert takes: "cache" (ApplyCache: the key's
// cached answers dropped), then "cut" (ApplyCut: Live.Cut). f must not call
// back into a. Nil stops it.
func (a *Authenticator) TraceRevocations(f func(ev, key string)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.tr = f
}
