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
	returned := make(chan error, 1)
	go func() { returned <- Watch(ctx, path, func() { called <- struct{}{} }, nil) }()
	// A bound only against a hang: the call back comes Settle after the
	// watch is set, which on a loaded machine is seconds late (10 Settles
	// once were not enough).
	select {
	case <-called:
	case err := <-returned:
		t.Fatalf("Watch returned %v before it called back", err)
	case <-time.After(time.Minute):
		t.Fatal("no call back once the watch was set")
	}
}
