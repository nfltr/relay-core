// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

// moveEvent is one line of a move's trace (trace_move_on.go): ev the step
// of spec/Move.tla, r a request (its transaction id), m what A received
// (RecvA: CONNECTED, EOF or a transaction id), ok a Pick's lease taken or
// a Publish on a live consumer.
type moveEvent struct {
	Ev string `json:"ev"`
	R  string `json:"r"`
	M  string `json:"m"`
	OK bool   `json:"ok"`
}
