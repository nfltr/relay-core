// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
)

// relay-server spec/README.md finding M6: on a SIGTERM drain without peers
// the agents go once the clients drained (AgentsGo), the relay's own
// services (Config.Uncounted: the accounts service the last usage flush
// reaches) only once that flush is done (ServicesGo); an agent other than
// a service is refused a new session once AgentsGo closed, so nothing
// counts after the last flush.
func servicesGoDispatcher(t *testing.T) (*Dispatcher, chan struct{}, chan struct{}) {
	t.Helper()
	d := NewDispatcher(t.Context(), fake.TestLogger(), broker.NewInMemoryMessageBroker(), nil, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 0, DispatcherConfig{
		ConnectedAgentsThreshold: 100,
		HeartbeatRecvThreshold:   5 * time.Second,
		RecvBufferSize:           10,
		Uncounted:                func(id string) bool { return id == "svc" },
	})
	agentsGo, servicesGo := make(chan struct{}), make(chan struct{})
	d.AgentsGo, d.ServicesGo = agentsGo, servicesGo
	return d, agentsGo, servicesGo
}

func TestServicesGoOnlyWithServicesGo(t *testing.T) {
	d, _, servicesGo := servicesGoDispatcher(t)
	close(servicesGo)                                               // AgentsGo stays open: only the services may go
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second) // only against a hang
	defer cancel()
	if err := d.WatchAgentSession(ctx, "svc", nil, nil, nil); !errors.Is(err, errServerRestarting) {
		t.Fatalf("the service's session: %v, want it to go at ServicesGo", err)
	}
	tcp := &TCPDispatcher{leaving: d.AgentsGo, servicesLeaving: d.ServicesGo, uncounted: d.uncounted}
	if tcp.leavingFor("a") != d.AgentsGo || tcp.leavingFor("svc") != d.ServicesGo {
		t.Fatal("a TCP stream leaves at its departure: an agent's at AgentsGo, a service's at ServicesGo")
	}
}

func TestAgentsRefusedOnceAgentsGo(t *testing.T) {
	d, agentsGo, _ := servicesGoDispatcher(t)
	if d.agentsGone("a") {
		t.Fatal("an agent refused before AgentsGo closed")
	}
	close(agentsGo)
	if !d.agentsGone("a") || !d.refusesAgents(t.Context(), "a") {
		t.Fatal("an agent may start a session after AgentsGo closed: it would count after the last flush")
	}
	if d.agentsGone("svc") || d.refusesAgents(t.Context(), "svc") {
		t.Fatal("a service refused before ServicesGo")
	}
	d.ServicesGo = nil // one departure for all (nfltr-relay): unchanged
	if d.agentsGone("a") {
		t.Fatal("refused without ServicesGo")
	}
}
