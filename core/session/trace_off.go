// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package session

import "github.com/nfltr/relay/proto/pb"

// traceOn is false without the relaytrace tag: every trace call sits behind
// `if traceOn`, so the compiler drops it with its arguments, and the trace
// fields of a stream and a session take no room (trace_on.go has the
// recorder; spec/README.md "Trace validation").
const traceOn = false

type (
	traceStream struct{}
	traceSess   struct{}
)

func traceUp(*tcpAgentStream)                                         {}
func traceOpenSess(*tcpAgentStream, string, *tcpSession)              {}
func traceWrite(*tcpAgentStream, string, int)                         {}
func traceRead(*tcpSession, int)                                      {}
func traceEof(*tcpSession)                                            {}
func traceDown(*tcpAgentStream)                                       {}
func traceEnqueue(*tcpAgentStream, *pb.TCPData, <-chan struct{}) bool { return true }
func traceRecv(*tcpAgentStream, *pb.TCPData, func() error) error      { return nil }
func traceFinish(*tcpAgentStream, string, func())                     {}
func traced(*tcpAgentStream) bool                                     { return false }
