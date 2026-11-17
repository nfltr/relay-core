// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package keyfile

// The recorder without the relaytrace tag (trace_on.go has it): no-ops.

type tracer struct{}

func (*APIKeyStore) trace(event)                          {}
func (*APIKeyStore) traceAdd(Machine)                     {}
func (*APIKeyStore) traceLoad()                           {}
func (*APIKeyStore) traceLines(_, _ map[[32]byte]Machine) {}
func keyID([32]byte) string                               { return "" }
func codeID(string) string                                { return "" }
