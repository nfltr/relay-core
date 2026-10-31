// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
)

// TestChunkedProxyResponseEndsOnRevoke: on the gRPC path (Proxy), a
// response the agent streams in chunks is held from its first chunk and
// cut when the key is revoked.
func TestChunkedProxyResponseEndsOnRevoke(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 30*time.Second)
	t.Cleanup(viper.Reset)
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "alice")
	proxy := NewProxy(t.Context(), testLogger(), b, testRelayCookie)
	// A key's short request carries its principal, as the relay's
	// authMiddleware leaves it.
	authn := identity.NewAuthenticator("keys")
	alice := identity.Principal{Provider: "keys", Owner: "alice", Subject: "alice", AgentID: "alice"}
	key := alice.Key()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(authn.Carry(r.Context(), alice))
		r.Header.Set(session.AgentIDKey, "alice")
		r.Header.Set(session.RequestAtProxyHeader, fmt.Sprint(time.Now().UnixMilli()))
		r.Header.Set(session.TransactionIDHeader, "txn-held")
		proxy.ServeHTTP(w, r)
	})
	req := httptest.NewRequest(http.MethodGet, "/stream", nil)
	ended := make(chan any, 1)
	go func() {
		defer func() { ended <- recover() }()
		h.ServeHTTP(httptest.NewRecorder(), req)
	}()
	for deadline := time.Now().Add(5 * time.Second); !proxy.txnRegistry.IsRegistered("txn-held"); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the proxy never registered the request")
		}
	}
	if n := authn.Held(key); n != 0 {
		t.Fatalf("held %d before the response streamed", n)
	}
	headers := map[string]*pb.HeaderValueList{}
	chunking.SetChunkHeaders(headers, 0, false)
	chunk0 := &pb.HTTPProxyResponse{
		Response:   &pb.HTTPResponse{TransactionId: "txn-held", StatusCode: 200, Payload: []byte("a"), Headers: headers},
		Timestamps: &pb.Timestamps{},
		Routing:    &pb.RoutingInfo{AgentId: "alice"},
	}
	if err := proxy.fanoutResponse(&testBrokerMessage{raw: chunk0}, time.Now()); err != nil {
		t.Fatal(err)
	}
	waitHeld(t, authn, key, 1)
	authn.Revoke(key)
	select {
	case p := <-ended:
		if p != http.ErrAbortHandler {
			t.Fatalf("ended with %v, want the response cut (http.ErrAbortHandler)", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("still streaming 5s after its key was revoked")
	}
	waitHeld(t, authn, key, 0)
}

// waitHeld waits until key holds n requests.
func waitHeld(t *testing.T, a *identity.Authenticator, key string, n int) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); a.Held(key) != n; {
		if time.Now().After(deadline) {
			t.Fatalf("key holds %d requests, want %d", a.Held(key), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
