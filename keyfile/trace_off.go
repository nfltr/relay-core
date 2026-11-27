// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package keyfile

// The recorder without the relaytrace tag (trace_on.go has it): no-ops.

type tracer struct{}

func (*Store) trace(event)                          {}
func (*Store) traceLoad()                           {}
func (*Store) traceLines(_, _ map[[32]byte]Machine) {}
func keyID([32]byte) string                         { return "" }
func codeID(string) string                          { return "" }
