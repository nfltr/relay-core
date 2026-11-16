// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package nf1anon

import "net/netip"

// The recorder without the relaytrace tag (trace_on.go has it): no-ops.

type tracer struct{}

func (*Registry) traceLock() func()                { return nop }
func (*Registry) trace(netip.Addr, event, int) int { return 0 }
