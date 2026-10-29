// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package nondumpable

import (
	"syscall"
	"testing"
)

func TestProcessIsNonDumpable(t *testing.T) {
	r, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_GET_DUMPABLE, 0, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	if r != 0 {
		t.Fatalf("PR_GET_DUMPABLE = %d, want 0", r)
	}
}
