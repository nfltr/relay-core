// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package session

import "context"

// The failover recorder without the relaytrace tag (trace_failover_on.go
// has it): a no-op.

func traceFailover(*Dispatcher, context.Context, string, string, string) {}
