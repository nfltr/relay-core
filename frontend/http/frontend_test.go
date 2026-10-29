// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"testing"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/session"
)

// TestAgentGoneDropsProducer: the mounted AgentGone drops the agent's
// request producer, which the relay calls on every way an agent session
// ends (relay.TestAgentRequestProducerRemovedOnEveryDisconnect): a dropped
// producer is closed and the next request gets a new one.
func TestAgentGoneDropsProducer(t *testing.T) {
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("", true)
	ing := newTestIngress(&session.Core{Broker: b})
	opts := broker.ProducerOptions{Topic: "agent-1"}
	before, release, err := ing.proxy.producers.Get("agent-1", b, opts) // a request to the agent
	if err != nil {
		t.Fatal(err)
	}
	release()
	ing.Mount().AgentGone("agent-1")
	after, release, err := ing.proxy.producers.Get("agent-1", b, opts)
	if err != nil {
		t.Fatal(err)
	}
	release()
	if after == before {
		t.Fatal("the departed agent's producer is still cached")
	}
}

// TestAttachMountsTheIngress: Attach mounts every part the relay serves.
func TestAttachMountsTheIngress(t *testing.T) {
	h := &testHost{core: &session.Core{Broker: broker.NewInMemoryMessageBroker()}}
	if err := (Frontend{}).Attach(t.Context(), h); err != nil {
		t.Fatal(err)
	}
	if h.mounted.Serve == nil || h.mounted.Peer == nil || h.mounted.Fanout == nil || h.mounted.AgentGone == nil {
		t.Fatalf("mounted %+v", h.mounted)
	}
}
