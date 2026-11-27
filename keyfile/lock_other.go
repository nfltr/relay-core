// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package keyfile

// lockFile takes no lock: relays run on Unix.
// ponytail: no cross-process lock off Unix; LockFileEx if a relay ever runs on Windows.
func lockFile(string) (func(), error) { return func() {}, nil }
