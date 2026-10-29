// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/spf13/viper"
)

func newURLTestServer(t *testing.T, inv directory.IPInventory) *Ingress {
	t.Helper()
	viper.Set("peer_link.port", 7443)
	t.Cleanup(viper.Reset)
	b := broker.NewInMemoryMessageBroker()
	s := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})
	s.core.Dispatcher = session.NewDispatcher(t.Context(), testLogger(), b, &testProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second,
		session.DispatcherConfig{ConnectedAgentsThreshold: 100})
	return s
}

// An agent on another replica is reached on that replica's link port,
// whatever else its record says: there is no other environment to forward to.
func TestGetDispatcherURLNamesTheAgentsReplica(t *testing.T) {
	s := newURLTestServer(t, &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "10.0.0.7"})
	u, err := s.dispatcherURL(context.Background(), "acct.node")
	if err != nil || u == nil || u.Host != "10.0.0.7:7443" {
		t.Fatalf("getDispatcherURL = %v, %v; want http://10.0.0.7:7443", u, err)
	}
	if _, err := newURLTestServer(t, &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "10.0.0.1%zz"}).dispatcherURL(context.Background(), "x"); err == nil {
		t.Fatal("an unparsable replica address resolved")
	}
}

// A label "pool=<name>" is an agent label like any other: a request for
// <name> does not reach the agent that carries it (implicit pools are gone).
func TestPoolLabelDoesNotRoute(t *testing.T) {
	s := newURLTestServer(t, &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "10.0.0.7"})
	member := &session.HTTPStream{ID: "acct.web-1"}
	member.SetLabels(map[string]string{"pool": "web"})
	s.core.Dispatcher.Agents.Store("acct.web-1", member)
	u, err := s.dispatcherURL(context.Background(), "web")
	if err != nil || u == s.loopbackURL {
		t.Fatalf("a request for the pool name web was served by its member: %v, %v", u, err)
	}
}

// podInventory names one replica (podIP) for every agent.
type podInventory struct {
	directory.IPInventory
	podIP string
}

func (m *podInventory) GetIP(context.Context, string) (string, error) { return m.podIP, nil }
