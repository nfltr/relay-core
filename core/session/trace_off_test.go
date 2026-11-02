// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package session

import (
	"testing"
	"unsafe"
)

// TestTraceOffCostsNothing: without -tags relaytrace the recorder is a
// constant false (every call compiled away) and its fields take no room.
func TestTraceOffCostsNothing(t *testing.T) {
	if traceOn || unsafe.Sizeof(traceStream{}) != 0 || unsafe.Sizeof(traceSess{}) != 0 || unsafe.Sizeof(traceMove{}) != 0 {
		t.Fatal("the trace recorder is on, or takes room, without -tags relaytrace")
	}
}
