// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package tracehook carries the trace recorder's hooks (-tags relaytrace,
// spec/README.md "Trace validation") between packages without a public
// API: without the tag nothing sets them and each runs its step as is.
package tracehook

// Publish runs publish, a request's first message onto agentID's topic
// (spec/Move.tla Publish), for the dispatcher d (a *session.Dispatcher);
// core/session's move recorder sets it to record the step.
var Publish = func(_ any, _, _ string, publish func() error) error { return publish() }

// RoutePeer runs route (session.Dispatcher.RouteHere) for a request a peer
// replica forwarded to agentID (spec/Move.tla ArriveA, not Pick), for the
// dispatcher d; core/session's move recorder sets it to record the step.
var RoutePeer = func(_ any, agentID string, route func(string) (func(), bool)) (func(), bool) {
	return route(agentID)
}
