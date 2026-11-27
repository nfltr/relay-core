// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package nf1cap

// The recorder without the relaytrace tag (trace_on.go has it): a no-op.

type tracer struct{}

func (*Registry) trace(rid, event) {}
