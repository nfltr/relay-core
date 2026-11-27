// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package repocheck

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSourceFilesCarryLicenseHeader: every Go file, shell script and
// Makefile of the module carries the SPDX Apache-2.0 header in its first
// lines.
func TestSourceFilesCarryLicenseHeader(t *testing.T) {
	err := filepath.WalkDir("../..", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "testdata" || d.Name() == ".git") {
			return filepath.SkipDir
		}
		switch filepath.Ext(path) {
		case ".go", ".sh":
		default:
			if filepath.Base(path) != "Makefile" && filepath.Base(path) != "pre-push" {
				return nil
			}
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.SplitN(string(data), "\n", 5)
		if !strings.Contains(strings.Join(lines[:min(4, len(lines))], "\n"), "SPDX-License-Identifier: Apache-2.0") {
			t.Errorf("%s: no SPDX-License-Identifier: Apache-2.0 header in its first lines", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
