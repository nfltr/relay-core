// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package tcprig

import (
	"io"
	"testing"
)

// TestPatternIsTheModulusStream: PatternReader yields byte(off % 251) at
// every offset whatever the read sizes, and PatternMismatch finds the
// first corrupt byte (or none) at any offset.
func TestPatternIsTheModulusStream(t *testing.T) {
	const n = 300 << 10
	want := make([]byte, n)
	for i := range want {
		want[i] = byte(i % 251)
	}
	got := make([]byte, 0, n)
	p := &PatternReader{}
	for _, size := range []int{1, 250, 251, 252, 7, 64 << 10, 65<<10 + 3, 1 << 20} {
		b := make([]byte, min(size, n-len(got)))
		if _, err := io.ReadFull(p, b); err != nil {
			t.Fatal(err)
		}
		got = append(got, b...)
	}
	if string(got) != string(want[:len(got)]) {
		t.Fatal("PatternReader is not byte(off % 251)")
	}
	for _, off := range []int{0, 1, 250, 251, 70000} {
		b := append([]byte(nil), want[off:off+100<<10]...)
		if i := PatternMismatch(b, off); i != -1 {
			t.Fatalf("offset %d: mismatch at %d in the pattern", off, i)
		}
		for _, bad := range []int{0, 1, 65 << 10, len(b) - 1} {
			b[bad]++
			if i := PatternMismatch(b, off); i != bad {
				t.Fatalf("offset %d: corrupt byte %d found at %d", off, bad, i)
			}
			b[bad]--
		}
	}
}
