// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
)

// TestProxyStreamedHeadReachesClientBeforeBody: a streamed response (SSE,
// a long poll) whose agent sent its head as chunk 0 with no body reaches
// the client at once on the gRPC proxy path, through the writers the
// relay wraps it in (the header policy, the front's, the metering one).
// The body never comes here: a head held until the body never arrives.
func TestProxyStreamedHeadReachesClientBeforeBody(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 30*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)
	policy := &HeaderPolicy{addResponseHeaders: map[string]string{"X-Policy": "on"}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set(session.AgentIDKey, "agent-head")
		r.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
		r.Header.Set(session.TransactionIDHeader, "txn-head")
		w = metrics.CountHTTP(w, r, metrics.PathHTTPGRPC, time.Now())
		w = util.NewResponseWriter(w, nil)
		HeaderRewriteMiddleware(policy, proxy).ServeHTTP(w, r)
	}))
	defer srv.Close()

	go func() {
		for !proxy.txnRegistry.IsRegistered("txn-head") {
			time.Sleep(5 * time.Millisecond)
		}
		h := map[string]*pb.HeaderValueList{"Content-Type": {HeaderValues: []string{"text/event-stream"}}}
		chunking.SetChunkHeaders(h, 0, false)
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: &pb.HTTPProxyResponse{
			Response:   &pb.HTTPResponse{TransactionId: "txn-head", StatusCode: 200, Headers: h},
			Routing:    &pb.RoutingInfo{AgentId: "agent-head"},
			Timestamps: &pb.Timestamps{},
		}}, time.Now())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // only a hang bound
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("no head before the body: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("X-Policy") != "on" {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
}
