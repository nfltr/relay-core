// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/internal/platform/txnregistry"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
)

// stuckResponseWriter is a browser that stops reading once the response's
// headers are out: Write blocks until released.
type stuckResponseWriter struct {
	header  http.Header
	release chan struct{}
}

func (w *stuckResponseWriter) Header() http.Header { return w.header }
func (w *stuckResponseWriter) WriteHeader(int)     {}
func (w *stuckResponseWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

// A browser that stops reading an agent's chunked response (an older agent:
// no download ack window) must not hold the broker fanout worker that every
// response of the pod goes through: past the stall bound its response is
// refused, the worker goes on, and the browser's connection is cut
// (http.ErrAbortHandler), never ended as if the body were whole. Before, the
// worker waited until the request ended, which a stuck write never does.
func TestProxyStuckBrowserDoesNotHoldTheFanoutWorker(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)
	proxy.txnRegistry.Stall = 50 * time.Millisecond

	// The agent's topic: every stalled chunk sends a cancel frame from a
	// goroutine of its own; the test ends only after the last one is sent,
	// so none reads viper while the deferred Reset writes it.
	topic := "agentID-1"
	_ = b.CreateTopic(topic, 0, true)
	agent, err := b.Subscribe(broker.ConsumerOptions{Topic: topic, SubscriptionName: "agentID-1", Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	cancels := make(chan struct{}, 64)
	go func() {
		for m := range agent.Chan() {
			if r, ok := m.RawObject().(*pb.HTTPProxyRequest); ok && chunking.IsCancel(r.GetRequest().GetHeaders()) {
				cancels <- struct{}{}
			}
		}
	}()

	const txnID = "txn-stuck-browser"
	req, _ := http.NewRequest("GET", "/download", strings.NewReader(""))
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, txnID)
	w := &stuckResponseWriter{header: http.Header{}, release: make(chan struct{})}

	served := make(chan any, 1)
	go func() {
		defer func() { served <- recover() }()
		proxy.ServeHTTP(w, req)
	}()
	for range 500 {
		if proxy.txnRegistry.IsRegistered(txnID) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	chunk := func(i int) *testBrokerMessage {
		return &testBrokerMessage{raw: &pb.HTTPProxyResponse{
			Response: &pb.HTTPResponse{
				TransactionId: txnID,
				StatusCode:    200,
				Payload:       []byte("chunk"),
				Headers:       map[string]*pb.HeaderValueList{chunking.HeaderIndex: {HeaderValues: []string{fmt.Sprint(i)}}},
			},
			Timestamps: &pb.Timestamps{},
			Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
		}}
	}
	worker := make(chan struct{})
	stalled := 0
	go func() {
		defer close(worker)
		for i := range 40 {
			if errors.Is(proxy.fanoutResponse(chunk(i), time.Now()), txnregistry.ErrTransactionStalled) {
				stalled++
			}
		}
	}()
	select {
	case <-worker:
	case <-time.After(5 * time.Second):
		t.Fatal("a browser that stopped reading holds the fanout worker")
	}

	close(w.release)
	select {
	case r := <-served:
		if r != http.ErrAbortHandler {
			t.Fatalf("the refused response ended with %v, want the connection cut (http.ErrAbortHandler)", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the refused response's handler did not end")
	}
	for n := range stalled + 1 { // each stalled chunk's, and the handler's own on its way out
		select {
		case <-cancels:
		case <-time.After(5 * time.Second):
			t.Fatalf("the agent got %d cancel frames; want %d (one per stalled chunk, one from the handler)", n, stalled+1)
		}
	}
}
