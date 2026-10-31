// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay/proto/pb"
)

// An agent that leaves leaves nothing keyed by its id in the broker: once
// its session ended and the proxy's producer for it closed, the broker
// has forgotten its topic (CreateTopic makes it anew). Before, every
// distinct agent id the relay had seen kept a topic and a closed consumer
// with its channel: about 1.2 KB each, for good.
func TestAgentThatLeftLeavesNoTopic(t *testing.T) {
	d, cancel := newTestDispatcher(t)
	defer cancel()
	for i := range 5 {
		id := fmt.Sprintf("gone-agent-%d", i)
		client, ctx := dialDispatcher(t, serveDispatcherAs(t, d, id))
		ctx, stop := context.WithCancel(ctx)
		s, err := client.ConnectHTTP(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Header(); err != nil {
			t.Fatal(err)
		}
		p, err := d.broker.CreateProducer(broker.ProducerOptions{Topic: id}) // the proxy's, for a visit
		if err != nil {
			t.Fatal(err)
		}
		stop() // the agent goes
		for deadline := time.Now().Add(5 * time.Second); d.IsAgentConnected(id); time.Sleep(5 * time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatalf("%s: session never ended", id)
			}
		}
		_, err = p.SendProto(context.Background(), &pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: "late"}, Timestamps: &pb.Timestamps{}})
		if !errors.Is(err, broker.ErrNoConsumer) {
			t.Fatalf("%s: a send after the agent left: %v, want ErrNoConsumer", id, err)
		}
		p.Close() // the proxy drops it (AgentGone, or its cache expiry)
		if err := d.broker.CreateTopic(id, 0, false); err != nil {
			t.Fatalf("%s: the broker still keeps its topic: %v", id, err)
		}
	}
}
