// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package filewatch

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fsnotify/fsnotify"
)

// TestAddSurvivesAVanishingSibling: a file beside the watched one that is
// created and removed while the watch is set (a sqlite journal next to the
// API key file) must not fail the watch. On macOS fsnotify's Add lstat's
// every entry of the directory and returned that entry's "no such file or
// directory" (2 of 3 connect conformance runs: the relay never applied a
// new key).
func TestAddSurvivesAVanishingSibling(t *testing.T) {
	dir := t.TempDir()
	for i := range 200 { // a wider window between ReadDir and each lstat
		if err := os.WriteFile(filepath.Join(dir, fmt.Sprint("stable-", i)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for c := range 4 {
		wg.Go(func() {
			name := filepath.Join(dir, fmt.Sprint("journal-", c))
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = os.WriteFile(name, nil, 0o600)
				_ = os.Remove(name)
			}
		})
	}
	defer func() { close(stop); wg.Wait() }()
	failed := 0
	const rounds = 300
	for range rounds {
		w, err := fsnotify.NewWatcher()
		if err != nil {
			t.Fatal(err)
		}
		if err := Add(w, dir); err != nil {
			failed++
			t.Log(err)
		}
		_ = w.Close()
	}
	if failed > 0 {
		t.Fatalf("Add failed %d of %d times while a sibling file came and went", failed, rounds)
	}
}
