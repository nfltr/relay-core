// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package filewatch

import (
	"errors"
	"io/fs"
	"os"

	"github.com/fsnotify/fsnotify"
)

// addAttempts bounds Add's retries. Each attempt reads the directory once;
// a journal that comes and goes beside the watched file fails one attempt,
// not 50 in a row (TestAddSurvivesAVanishingSibling churns four files
// without pause and needs a few).
const addAttempts = 50

// Add watches the directory dir with w, as w.Add does, but does not fail
// when an entry of dir vanishes while the watch is set: on macOS and the
// BSDs (kqueue) fsnotify's Add lstat's every entry of the directory and
// returns the first one's "no such file or directory" (fsnotify v1.10.1
// still), so a sqlite journal beside the API key file failed the key
// file's watch. It retries then, while dir itself exists, at most
// addAttempts times. Every directory watch in the relay goes through it.
func Add(w *fsnotify.Watcher, dir string) error {
	var err error
	for range addAttempts {
		if err = w.Add(dir); err == nil || !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if _, statErr := os.Stat(dir); statErr != nil {
			return err // the directory itself is gone
		}
		_ = w.Remove(dir) // the failed Add left dir half watched
	}
	return err
}
