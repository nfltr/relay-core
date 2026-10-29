// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
)

// A request sent the moment its agent counts as connected here reaches
// the agent: the session subscribes to the agent's topic before it
// registers. Before, a request routed in between (IsAgentConnected already
// true) was published to no subscriber, dropped without an error, and its
// visitor waited for the request timeout: about one visit per rolling
// restart in the kind soak, as agents moved onto new replicas under load
// (2026-10-17).
func TestRequestRoutedAsTheAgentRegistersReachesIt(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	published := make(chan error, 1)
	d.OnAgentMetadata = func(info AgentInfo) { // the session has just registered
		if !d.IsAgentConnected(info.AgentID) {
			published <- nil
			return
		}
		p, err := d.broker.CreateProducer(broker.ProducerOptions{Topic: info.AgentID})
		if err == nil {
			_, err = p.SendProto(context.Background(), &pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: "txn-at-register", Method: "GET", Url: "x"}, Timestamps: &pb.Timestamps{}})
		}
		published <- err
	}
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "reg-agent"))
	ctx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	s, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for {
		msg, err := s.Recv()
		if err != nil {
			t.Fatalf("the request routed as the agent registered never reached it: %v", err)
		}
		if msg.GetRequest().GetTransactionId() == "txn-at-register" {
			break
		}
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}
}

// teardownInventory calls onDelete as a session's teardown removes its
// record: after its consumer closed, while it still counts as connected.
type teardownInventory struct {
	directory.IPInventory
	onDelete func(agentID string)
}

func (i *teardownInventory) DeleteIPs(ctx context.Context, agentIDs ...string) error {
	for _, id := range agentIDs {
		i.onDelete(id)
	}
	return i.IPInventory.DeleteIPs(ctx, agentIDs...)
}

// A request routed to an agent as its session ends is never dropped
// without a word: routed while the agent still counts as connected here,
// after its session stopped taking requests, its publish fails (the proxy
// answers the visitor 503 at once). Before, the in-memory broker took it
// without an error and its visitor waited for the request timeout: the
// kind soak's residual at rolling restarts (2026-10-17).
func TestRequestRoutedAsTheSessionEndsIsNotDropped(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	published := make(chan error, 1)
	d.IPInventory = &teardownInventory{IPInventory: d.IPInventory, onDelete: func(agentID string) {
		if !d.IsAgentConnected(agentID) {
			return
		}
		p, err := d.broker.CreateProducer(broker.ProducerOptions{Topic: agentID})
		if err == nil {
			_, err = p.SendProto(context.Background(), &pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: "txn-at-end", Method: "GET", Url: "x"}, Timestamps: &pb.Timestamps{}})
		}
		select {
		case published <- err:
		default:
		}
	}}
	client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, "end-agent"))
	ctx, stop := context.WithCancel(ctx)
	s, err := client.ConnectHTTP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Header(); err != nil {
		t.Fatal(err)
	}
	stop() // the agent goes
	select {
	case err := <-published:
		if !errors.Is(err, broker.ErrNoConsumer) {
			t.Fatalf("a request routed as the session ended was published with %v: dropped without a word", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the session's teardown never ran")
	}
}

// Requests the session's consumer took but never handed to the agent are
// answered agent-gone when the session ends: their visitors get the answer
// at once, not the request timeout.
func TestQueuedRequestsAreAnsweredWhenTheSessionEnds(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	b := broker.NewInMemoryMessageBroker()
	if err := b.CreateTopic("q-agent", 1, false); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe(broker.ConsumerOptions{Topic: "q-agent", SubscriptionName: "q-agent", Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	c.Chan()
	p, err := b.CreateProducer(broker.ProducerOptions{Topic: "q-agent"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.SendProto(t.Context(), &pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: "queued", Method: "GET", Url: "x"}, Timestamps: &pb.Timestamps{}}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	pp := &fake.TestProducer{}
	d.podIDTopicProducer = pp
	d.failQueued(&HTTPStream{ID: "q-agent"}, c.Chan())
	resp, _ := pp.LastData.(*pb.HTTPProxyResponse)
	if resp.GetResponse().GetTransactionId() != "queued" || !IsAgentGone(resp.GetResponse()) {
		t.Fatalf("answered %v; want agent-gone for the queued request", pp.LastData)
	}
}
