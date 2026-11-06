// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharecode

import (
	"strings"
	"testing"
)

// TestValid accepts the current base32 form and the earlier
// word-pair-hex form (links already issued), and refuses everything else.
func TestValid(t *testing.T) {
	hex32 := strings.Repeat("0a", 16)
	for _, code := range []string{
		"aaaaaaaaaaaaaaaaaaaaaaaaaa",
		"7777777777777777777777777q", // last char carries 3 bits
		"swift-bay-" + hex32,
		"bright-canyon-" + strings.Repeat("f", 32),
	} {
		if !Valid(code) {
			t.Errorf("Valid(%q) = false", code)
		}
	}
	for _, code := range []string{
		"", "swift-bay", "swift-bay-a1b2c3", "docs", "api", "b-0123456789abcdef",
		"AAAAAAAAAAAAAAAAAAAAAAAAAA",       // uppercase
		"aaaaaaaaaaaaaaaaaaaaaaaaab",       // nonzero trailing bits
		"aaaaaaaaaaaaaaaaaaaaaaaaa",        // 25 chars
		"aaaaaaaaaaaaaaaaaaaaaaaaaaa",      // 27 chars
		"aaaaaaaaaaaaaaaaaaaaaaaa1a",       // '1' is not base32
		"aaaaaaaaaaaaaaaaaaaaaaaaaa======", // padding
		"swift-bay-" + strings.ToUpper(hex32),
		"swift-bay-" + hex32 + "0",
		"swift-" + hex32,
		"swift-b4y-" + hex32,
		"-bay-" + hex32,
		"a-swift-bay-" + hex32,
		"../../" + hex32,
	} {
		if Valid(code) {
			t.Errorf("Valid(%q) = true", code)
		}
	}
}

// TestID (METER3): a share's id is stable for its code, differs between
// codes, and does not contain the code.
func TestID(t *testing.T) {
	a, b := New(), New()
	if ID(a) != ID(a) || ID(a) == ID(b) {
		t.Fatalf("ID(%q)=%q, ID(%q)=%q: want stable and distinct", a, ID(a), b, ID(b))
	}
	if len(ID(a)) != 16 || strings.Contains(ID(a), a) || strings.Contains(a, ID(a)) {
		t.Fatalf("ID(%q) = %q: want 16 hex characters, not the code", a, ID(a))
	}
}
