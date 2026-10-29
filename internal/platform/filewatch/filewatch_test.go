// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package filewatch

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWatchCatchesAChangeBeforeTheWatch: a file changed after the caller
// read it but before the watch was set has no event; Watch calls back once
// when the watch is set, so the caller reads it again.
func TestWatchCatchesAChangeBeforeTheWatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(path, []byte("changed before the watch"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	called := make(chan struct{}, 1)
	go func() { _ = Watch(ctx, path, func() { called <- struct{}{} }, nil) }()
	select {
	case <-called:
	case <-time.After(10 * Settle):
		t.Fatal("no call back once the watch was set")
	}
}
