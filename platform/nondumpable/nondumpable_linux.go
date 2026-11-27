// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package nondumpable

import "syscall"

func init() { _, _, _ = syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_DUMPABLE, 0, 0) }
