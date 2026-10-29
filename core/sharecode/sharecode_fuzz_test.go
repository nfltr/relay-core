// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package sharecode

import (
	"strings"
	"testing"
)

// FuzzValidCode fuzzes the share-code check every store's Issue and
// Validate run on untrusted codes (URL path, SNI label): no panic, the
// answer is deterministic, an accepted code is either the 26-char base32
// form or the word-pair + 32-hex form, and generated codes validate.
func FuzzValidCode(f *testing.F) {
	for _, seed := range []string{
		"", "-", "fresh-crest", "fresh-crest-a1b2c3", New(),
		"fresh-crest-" + strings.Repeat("0", legacyHexLen),
		"fresh-crest-" + strings.Repeat("A", legacyHexLen),
		"-" + strings.Repeat("0", legacyHexLen),
		strings.ToUpper(New()), New() + "=",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, code string) {
		ok := Valid(code)
		if ok != Valid(code) {
			t.Fatalf("%q: not deterministic", code)
		}
		if ok {
			base32Form := len(code) == codeLen && strings.Trim(code, "abcdefghijklmnopqrstuvwxyz234567") == ""
			parts := strings.Split(code, "-")
			legacyForm := len(parts) == 3 && len(parts[2]) == legacyHexLen &&
				strings.Trim(parts[2], "0123456789abcdef") == ""
			if !base32Form && !legacyForm {
				t.Fatalf("%q accepted", code)
			}
		}
		if gen := New(); !Valid(gen) {
			t.Fatalf("generated %q rejected", gen)
		}
	})
}
