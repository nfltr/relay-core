// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"os"
	"testing"

	"github.com/nfltr/relay-core/internal/platform/agentcliguard"
	"github.com/nfltr/relay-core/internal/platform/leakcheck"
)

// TestMain keeps these tests off every real relay and agent CLI the host or
// a node agent's environment points at.
func TestMain(m *testing.M) {
	// Known leftovers found when the leak check was added, narrowed to their
	// frames; each needs its test to end the stub stream it feeds (follow-up:
	// end them in t.Cleanup and drop the entry):
	//  - routeAgentToAgentLocal: agent-to-agent routing tests whose stub agent streams never end.
	//  - TCPDispatcher.recvLoop (serveAux): aux-stream tests that never end the aux stream.
	//  - presenceFeed.BindBus: the bus subscription outlives the feed it was bound to.
	//  - peerlink Node.Drain: the github.com/nfltr/link goroutine described in relay client/agent's leak_main_test.go.
	//  - TestProcessGRPCInput_NilRouting, TestDialAgentWithRetry_SucceedsOnRetry,
	//    TestDialAgent_WriteToJoinsBacklog, TestDialAgent_CloseAfterDataDeliversEverything:
	//    helper goroutines of those tests that no cleanup stops.
	//  - peerlinktest.*: github.com/nfltr/link's conformance helpers (testStuckReader, Proxy.pipe); follow-up in link.
	os.Exit(leakcheck.RunFunc(func() int { return agentcliguard.Run(m) },
		"session.TestProcessGRPCInput_NilRouting", "session.TestDialAgentWithRetry_SucceedsOnRetry",
		"session.TestDialAgent_WriteToJoinsBacklog", "session.TestDialAgent_CloseAfterDataDeliversEverything",
		"peerlinktest.",
		"(*Dispatcher).routeAgentToAgentLocal", "(*TCPDispatcher).recvLoop",
		"presenceFeed).BindBus", "peerlink.(*Node).Drain"))
}
