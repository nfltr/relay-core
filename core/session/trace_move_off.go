// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package session

// The move's recorder without the relaytrace tag (trace_move_on.go has it):
// every step runs as is, and a stream's trace takes no room.

type traceMove struct{}

func traceMoveStep(_ *HTTPStream, _ moveEvent, change func() bool) bool    { return change() }
func traceForward(_ *HTTPStream, _ string, send func() error) error        { return send() }
func traceAnswer(_ *HTTPStream, _ string, deliver func())                  { deliver() }
func traceMoveEOF(*HTTPStream)                                             {}
func traceMoveEnd(*HTTPStream)                                             {}
func traceTCPMoving(*Dispatcher, string, *tcpAgentStream, bool)            {}
func traceTCPStep(_ *tcpAgentStream, _, _ string, change func() bool) bool { return change() }
func (d *Dispatcher) traceRouteHere(string) (func(), bool)                 { return nil, false }
