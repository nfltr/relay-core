// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
)

// A response chunk for a transaction nobody here waits for (its visitor
// left and the cancel the proxy sent then was lost) tells the agent again
// to stop it: the agent stops sending, and the dispatcher, seeing the
// cancel, no longer counts it in flight for the rest of the session. A
// last chunk ends the transaction and needs none. Before, only a stalled
// visitor got a second cancel.
func TestLateChunkCancelsItsTransaction(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	t.Cleanup(viper.Reset)
	b := broker.NewInMemoryMessageBroker()
	t.Cleanup(b.Close)
	_ = b.SetTopicAutoCreation("ns", true)
	agent, err := b.Subscribe(broker.ConsumerOptions{Topic: "agent-late", SubscriptionName: "agent-late"})
	if err != nil {
		t.Fatal(err)
	}
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)
	chunk := func(txn string, last bool) *testBrokerMessage {
		h := map[string]*pb.HeaderValueList{}
		chunking.SetChunkHeaders(h, 3, last)
		return &testBrokerMessage{raw: &pb.HTTPProxyResponse{
			Response:   &pb.HTTPResponse{TransactionId: txn, Headers: h},
			Timestamps: &pb.Timestamps{}, Routing: &pb.RoutingInfo{AgentId: "agent-late"}}}
	}
	_ = proxy.fanoutResponse(chunk("txn-last", true), time.Now())
	_ = proxy.fanoutResponse(chunk("txn-gone", false), time.Now())
	select {
	case m := <-agent.Chan():
		req, err := broker.DecodeBrokerMessage(m, &pb.HTTPProxyRequest{})
		if err != nil || !chunking.IsCancel(req.GetRequest().GetHeaders()) || req.GetRequest().GetTransactionId() != "txn-gone" {
			t.Fatalf("the agent got %v (%v), want the cancel of txn-gone", req, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no cancel for a chunk of a transaction nobody waits for")
	}
	select {
	case m := <-agent.Chan():
		t.Fatalf("a second request reached the agent: %v", m)
	case <-time.After(100 * time.Millisecond):
	}
}
