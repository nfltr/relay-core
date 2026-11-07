// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"crypto/sha256"
	"encoding/base32"
	"slices"

	"github.com/nfltr/relay-core/core/identity"
)

// tm is a fixed machine id per test name.
func tm(name string) identity.MachineID {
	sum := sha256.Sum256([]byte(name))
	return identity.MachineID("m-" + base32Lower(sum[:16]))
}

func base32Lower(b []byte) string {
	return base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding).EncodeToString(b)
}

// tmach is test machine name, bound to service svc.
func tmach(name string) Machine { return Machine{ID: tm(name), Name: name, Services: []string{"svc"}} }

// kl is the key file line of test machine name with key.
func kl(name, key string) string { return tmach(name).line(sha256.Sum256([]byte(key))) }

// ids is the subjects (machine ids) of test machines names.
func ids(names ...string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = string(tm(n))
	}
	return out
}

// sortedIDs is ids sorted, as Reload returns its revoked machines.
func sortedIDs(names ...string) []string { return slices.Sorted(slices.Values(ids(names...))) }
