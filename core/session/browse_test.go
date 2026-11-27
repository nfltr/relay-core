// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"
)

func TestBrowsePathPrefix(t *testing.T) {
	if BrowsePathPrefix != "/browse/" {
		t.Fatalf("BrowsePathPrefix: got %q, want %q", BrowsePathPrefix, "/browse/")
	}
}
