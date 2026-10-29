// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package nondumpable makes the importing process non-dumpable on Linux
// from process start (package init, before any main code runs), so no other
// process of the same uid without CAP_SYS_PTRACE can read the process's
// /proc/<pid>/environ (which keeps an exec-time NFLTR_API_KEY after
// os.Unsetenv) or its memory. execve resets the flag, so children are
// unaffected. Elsewhere importing it does nothing.
//
// Every binary under cmd/ imports it blank (guarded by the repository's
// TestCmdBinariesNonDumpable); relayserver imports it too, so the hosted
// relay binaries that link relayserver are covered.
package nondumpable
