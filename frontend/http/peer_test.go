// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
)

// TestPeerForwardAfterMoveIsRefusedRetryable: a request a peer replica
// forwarded here (it read the agent's record before it moved) takes the
// route lease as this replica's own ingress does; once this replica
// stopped routing to the agent (its old session may still drain its
// topic) it is answered 503 Retry-After and never reaches the agent, which
// would run it after MOVE_DONE with its answer lost (spec/README.md
// finding 20, Move.tla ArriveA, mutant peer-no-lease).
func TestPeerForwardAfterMoveIsRefusedRetryable(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", time.Second)
	defer viper.Reset()
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("", true)
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: directory.NewNoOpInventory()})
	ing.core.Dispatcher = session.NewDispatcher(context.Background(), testLogger(), b, &testProducer{}, directory.NewNoOpInventory(), directory.NewNoOpInventory(), 5*time.Second, session.DispatcherConfig{ConnectedAgentsThreshold: 100})
	// The moved session's consumer still reads the agent's topic.
	_ = b.CreateTopic("agent-1", 0, true)
	c, err := b.Subscribe(broker.ConsumerOptions{Topic: "agent-1", SubscriptionName: "agent-1", Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set(session.AgentIDKey, "agent-1")
	r.Header.Set(session.TransactionIDHeader, "txn-1")
	r.Header.Set(session.RequestAtProxyHeader, strconv.FormatInt(time.Now().UnixMilli(), 10))
	w := httptest.NewRecorder()
	ing.Mount().Peer.ServeHTTP(w, r)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
		t.Errorf("forward to an agent this replica no longer routes to: %d, Retry-After %q; want 503 with Retry-After", w.Code, w.Header().Get("Retry-After"))
	}
	select {
	case m := <-c.Chan():
		t.Fatalf("the forward reached the agent's topic: %v", m)
	default:
	}
}
