// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package keyfile

import (
	"os"
	"syscall"
)

// lockFile holds an exclusive flock on path (created 0600) until the
// returned func runs; another process's lockFile waits for it.
func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600) // #nosec G304 -- beside the operator's key file
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
