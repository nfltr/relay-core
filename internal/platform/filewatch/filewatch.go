// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package filewatch calls back when a file changes, for settings the relay
// reloads without a restart (the API key file, the site hosts file).
package filewatch

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Settle is how long the file's directory stays quiet before Watch calls
// back: a rewrite or a ConfigMap swap is a burst of events.
const Settle = 300 * time.Millisecond

// Watch calls changed whenever path may have changed, until ctx ends. It
// watches the file's directory, not the file: an editor or an atomic write
// replaces the file, a Kubernetes ConfigMap swaps the symlink its entries
// point through (..data). Event-driven: no wakeup while nothing in the
// directory changes. A watch error is passed to failed (nil: ignored). It
// returns an error only when the directory cannot be watched. It calls back
// once after the watch is set, so the callback must tolerate no change.
func Watch(ctx context.Context, path string, changed func(), failed func(error)) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()
	if err := w.Add(filepath.Dir(path)); err != nil {
		return err
	}
	base := filepath.Base(path)
	// One call back once the watch is set: a change between the caller's
	// read and Add has no event (TestWatchCatchesAChangeBeforeTheWatch).
	settle := time.After(Settle)
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-w.Events:
			if !ok {
				return nil
			}
			if name := filepath.Base(ev.Name); name == base || strings.HasPrefix(name, "..") {
				settle = time.After(Settle)
			}
		case err, ok := <-w.Errors:
			if !ok {
				return nil
			}
			if failed != nil {
				failed(err)
			}
		case <-settle:
			settle = nil
			changed()
		}
	}
}
