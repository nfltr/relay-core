// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/session"
	"github.com/spf13/viper"
)

// The record names this replica (GetIP answers the loopback address) while
// the agent's session here has claimed it and not registered yet (spec
// finding 45, Move.tla ArriveB): the request waits for the registration and
// is served here, never forwarded to this replica's own link port.
func TestDispatchWaitsForAnArrivingAgent(t *testing.T) {
	s := newURLTestServer(t, &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "127.0.0.1"})
	s.core.Dispatcher.Config.LoopbackAddr = "127.0.0.1"
	registered := registerLater(s.core.Dispatcher, "acct.mover", 100*time.Millisecond)
	u, release, err := s.dispatcherURL(context.Background(), "acct.mover")
	if err != nil || u != s.loopbackURL {
		t.Fatalf("dispatcherURL while the agent registers here = %v, %v; want it served here (in-process)", u, err)
	}
	release()
	if waited := time.Since(<-registered); waited > time.Second {
		t.Fatalf("served %v after the registration; want woken by it", waited)
	}
}

// An agent whose record names this replica and that never registers here
// (a stale record) is offline: refused after the wait, never forwarded here.
func TestDispatchRefusesAStaleRecordOfThisReplica(t *testing.T) {
	s := newURLTestServer(t, &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "127.0.0.1"})
	s.core.Dispatcher.Config.LoopbackAddr = "127.0.0.1"
	u, _, err := s.dispatcherURL(context.Background(), "acct.gone")
	if err == nil {
		t.Fatalf("dispatcherURL for an agent not here whose record names here = %v; want refused", u)
	}
}

// A peer's forward arriving while the agent's session here registers is
// served once it registered, not refused (Ingress.peer, ArriveB).
func TestPeerForwardWaitsForAnArrivingAgent(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 2*time.Second)
	defer viper.Reset()
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("", true)
	inv := &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "127.0.0.1"}
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})
	ing.core.Dispatcher = session.NewDispatcher(context.Background(), testLogger(), b, &testProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second,
		session.DispatcherConfig{ConnectedAgentsThreshold: 100, LoopbackAddr: "127.0.0.1"})
	_ = b.CreateTopic("agent-1", 0, true)
	c, err := b.Subscribe(broker.ConsumerOptions{Topic: "agent-1", SubscriptionName: "agent-1", Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	registerLater(ing.core.Dispatcher, "agent-1", 100*time.Millisecond)

	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set(session.AgentIDKey, "agent-1")
	r.Header.Set(session.TransactionIDHeader, "txn-1")
	r.Header.Set(session.RequestAtProxyHeader, strconv.FormatInt(time.Now().UnixMilli(), 10))
	done := make(chan int, 1)
	go func() {
		w := httptest.NewRecorder()
		ing.Mount().Peer.ServeHTTP(w, r)
		done <- w.Code
	}()
	select {
	case <-c.Chan(): // the request reached the agent's topic
	case code := <-done:
		t.Fatalf("forward while the agent registers here answered %d before reaching the agent", code)
	case <-time.After(5 * time.Second):
		t.Fatal("the forward neither reached the agent nor was answered")
	}
}

// registerLater registers agentID's HTTP session on d after delay; the
// channel gets the moment it did.
func registerLater(d *session.Dispatcher, agentID string, delay time.Duration) <-chan time.Time {
	at := make(chan time.Time, 1)
	go func() {
		time.Sleep(delay)
		d.AddHTTPSession(agentID, &session.HTTPStream{ID: agentID})
		at <- time.Now()
	}()
	return at
}

// TestPeerForwardAfterMoveGoesOnToTheAgentsReplica: a request a peer
// forwarded here (it read the agent's record before the agent moved off
// this replica) that arrives once this replica stopped routing to the
// agent goes on, once, to the replica the record now names, never
// answered 503 while the agent is connected there (Move.tla ArriveA,
// MovedForwards, mutant peer-refuses-moved; spec/README.md finding 46).
// A request that already went on once is refused, not forwarded again.
func TestPeerForwardAfterMoveGoesOnToTheAgentsReplica(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", time.Second)
	defer viper.Reset()
	hops := make(chan string, 2)
	newReplica := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops <- r.Header.Get(peerHopHeader)
		_, _ = io.WriteString(w, "served by the agent's new replica")
	}))
	defer newReplica.Close()
	_, port, _ := net.SplitHostPort(newReplica.Listener.Addr().String())
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("", true)
	inv := &podInventory{IPInventory: directory.NewNoOpInventory(), podIP: "127.0.0.1"} // the record names the new replica
	ing := newTestIngress(&session.Core{Broker: b, DispatcherIPInventory: inv})
	ing.peerLinkPort = port
	ing.core.Dispatcher = session.NewDispatcher(context.Background(), testLogger(), b, &testProducer{}, inv, directory.NewNoOpInventory(), 5*time.Second,
		session.DispatcherConfig{ConnectedAgentsThreshold: 100, LoopbackAddr: "10.9.9.9"})

	req := func(hop string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/x", nil)
		r.Header.Set(session.AgentIDKey, "agent-1")
		r.Header.Set(session.TransactionIDHeader, "txn-1")
		if hop != "" {
			r.Header.Set(peerHopHeader, hop)
		}
		w := httptest.NewRecorder()
		ing.Mount().Peer.ServeHTTP(w, r)
		return w
	}
	if w := req(""); w.Code != http.StatusOK || w.Body.String() != "served by the agent's new replica" {
		t.Fatalf("forward after the agent moved: %d %q; want it served by the replica the record names", w.Code, w.Body.String())
	}
	if hop := <-hops; hop == "" {
		t.Error("the onward forward does not say it went on once")
	}
	if w := req("1"); w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") == "" {
		t.Errorf("a forward that already went on once: %d; want 503 Retry-After, not a second hop", w.Code)
	}
	select {
	case <-hops:
		t.Error("a forward that already went on once was forwarded again")
	default:
	}
}
