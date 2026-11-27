// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package identity

// revokeTrace records nothing without -tags relaytrace (trace_on.go).
type revokeTrace struct{}

func (revokeTrace) emit(string, string) {}
