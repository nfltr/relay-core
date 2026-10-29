// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/spf13/viper"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/session"
)

// TestProxyRequestBodyLimitEdge: a body of exactly the cap reaches the agent;
// one byte more gets a 413 that names the limit and the setting, before the
// agent sees anything when the length is declared.
func TestProxyRequestBodyLimitEdge(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	t.Cleanup(viper.Reset)
	for _, tc := range []struct {
		name     string
		limit    int64
		body     io.Reader
		length   int64 // -1: chunked, no Content-Length
		wantCode int
		wantText string
	}{
		{"1 GiB cap+1 declared", 1 << 30, unreadableBody{}, 1<<30 + 1, http.StatusRequestEntityTooLarge, "at most 1 GiB (http.server.max_request_body_size)"},
		{"at the cap", 1000, strings.NewReader(strings.Repeat("a", 1000)), 1000, http.StatusOK, "ok"},
		{"cap+1 streamed", 1000, strings.NewReader(strings.Repeat("a", 1001)), -1, http.StatusRequestEntityTooLarge, "at most 1000 bytes (http.server.max_request_body_size)"},
		{"cap+1 declared", 1000, unreadableBody{}, 1001, http.StatusRequestEntityTooLarge, "at most 1000 bytes (http.server.max_request_body_size)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if viper.GetInt64("http.server.max_request_body_size") != tc.limit {
				viper.Set("http.server.max_request_body_size", tc.limit)
			}
			b := broker.NewInMemoryMessageBroker()
			_ = b.SetTopicAutoCreation("ns", true)
			proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

			txn := "txn-limit-" + strings.ReplaceAll(tc.name, " ", "-")
			req := httptest.NewRequest(http.MethodPost, "/upload", tc.body)
			req.ContentLength = tc.length
			req.Header.Set(session.AgentIDKey, "agentID-1")
			req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
			req.Header.Set(session.TransactionIDHeader, txn)
			w := httptest.NewRecorder()
			if tc.wantCode == http.StatusOK {
				go func() {
					for range 100 {
						if proxy.txnRegistry.IsRegistered(txn) {
							break
						}
						time.Sleep(10 * time.Millisecond)
					}
					_ = proxy.fanoutResponse(&testBrokerMessage{raw: &pb.HTTPProxyResponse{
						Response:   &pb.HTTPResponse{TransactionId: txn, StatusCode: 200, Payload: []byte("ok")},
						Timestamps: &pb.Timestamps{},
						Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
					}}, time.Now())
				}()
			}
			proxy.ServeHTTP(w, req)
			if w.Code != tc.wantCode || !strings.Contains(w.Body.String(), tc.wantText) {
				t.Fatalf("got %d %q, want %d containing %q", w.Code, w.Body.String(), tc.wantCode, tc.wantText)
			}
		})
	}
}

type unreadableBody struct{}

func (unreadableBody) Read([]byte) (int, error) { return 0, errors.New("body read before the 413") }

// TestProxyNeverForwardsAShortBodyAsComplete: when the client's body ends
// in a read error before its Content-Length, the dispatcher answers 400 and
// cancels, instead of handing the agent the partial body as its last chunk
// (the chunk reader treats any read error as the end of the body).
func TestProxyNeverForwardsAShortBodyAsComplete(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	t.Cleanup(viper.Reset)
	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest(http.MethodPost, "/upload", io.MultiReader(strings.NewReader("abc"), errReader{io.ErrUnexpectedEOF}))
	req.ContentLength = 1000
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-short-body")
	w := httptest.NewRecorder()
	start := time.Now()
	proxy.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "incomplete request body") {
		t.Fatalf("got %d %q after %s, want 400 incomplete request body", w.Code, w.Body.String(), time.Since(start))
	}
}
