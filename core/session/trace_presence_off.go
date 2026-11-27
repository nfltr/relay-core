// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

//go:build !relaytrace

package session

import presencev1 "github.com/nfltr/relay/proto/nfltr/presence/v1"

// The presence recorder without the relaytrace tag (trace_presence_on.go
// has it): every call is a no-op behind traceOn and a feed's trace field
// takes no room.

type (
	tracePres      struct{}
	presTraceGroup struct{}
)

func tracePresenceStart(string) *presTraceGroup                            { return nil }
func tracePresenceJoin(*presenceFeed, *presTraceGroup, string)             {}
func tracePresBump(*presenceFeed, *presencev1.PresenceEvent, string)       {}
func tracePresResync(*presenceFeed)                                        {}
func tracePresWake(*presenceFeed, []*presencev1.PresenceEvent, bool)       {}
func tracePresCursor(*presenceFeed)                                        {}
func tracePresSnap(*presenceFeed, bool, map[string]string)                 {}
func tracePresRecord(*Dispatcher, string, string, bool, map[string]string) {}
