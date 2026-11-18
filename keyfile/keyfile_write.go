// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package keyfile

import (
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/nfltr/relay-core/core/identity"
)

// writeFileAtomic replaces path with data: written whole to a temporary
// file in the same directory, synced, then renamed over path, so a reader
// (a relay's Watch) sees the old file or the new one, never a part. The
// file keeps its mode; a new one is 0600.
func writeFileAtomic(path string, data []byte) (err error) {
	mode := os.FileMode(0o600)
	if fi, statErr := os.Stat(path); statErr == nil {
		mode = fi.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
		}
	}()
	if err = tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// editFile is every write of the key file: under an exclusive lock on
// <path>.lock, which every writer takes (the running relay and
// `nfltr-relay machines` in other processes), it reads the file as it is
// on disk, edits its lines and replaces it (writeFileAtomic). A writer so
// never drops a line another wrote since it loaded the file, nor brings
// back a line another removed.
func editFile(path string, edit func(lines []string) []string) error {
	unlock, err := lockFile(path + ".lock")
	if err != nil {
		return err
	}
	defer unlock()
	data, err := os.ReadFile(path) // #nosec G304 -- operator-configured key file
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(data) == 0 {
		lines = nil
	}
	lines = edit(lines)
	return writeFileAtomic(path, []byte(strings.Join(lines, "\n")+"\n"))
}

// dropMachine is lines without machine id's line (its first field).
func dropMachine(lines []string, id identity.MachineID) []string {
	return slices.DeleteFunc(lines, func(l string) bool {
		f := strings.Fields(l)
		return len(f) > 0 && f[0] == string(id)
	})
}
