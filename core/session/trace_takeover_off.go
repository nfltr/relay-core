// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package session

// The takeover recorder without the relaytrace tag (trace_takeover_on.go
// has it): no process names, each step only runs.

func traceTakeoverProc(*Dispatcher, string) string { return "" }

func traceTakeoverStep(_ *Dispatcher, _, _, _ string, step func() (string, string)) { step() }
