// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/chunking"
	"github.com/spf13/viper"
)

// --- PrepareAgentRequest ---

func TestPrepareAgentRequest(t *testing.T) {
	viper.Set("pod_id.env_var", "MY_POD_NAME")
	defer viper.Reset()

	body := strings.NewReader(`{"key":"value"}`)
	r := httptest.NewRequest("POST", "/wapi/v2.13.4/resource", body)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer token123")
	r.Header.Set("X-Custom", "custom-value")

	deadline := time.Now().Add(time.Minute)
	requestAtProxy := time.Now()

	req, reqBody, err := prepareRequest(r, "agentID-1", "txn-123", deadline, requestAtProxy)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer reqBody.Close() //nolint:errcheck
	if req.Request.TransactionId != "txn-123" {
		t.Fatalf("expected txn-123, got %s", req.Request.TransactionId)
	}
	if req.Routing.AgentId != "agentID-1" {
		t.Fatalf("expected agentID-1, got %s", req.Routing.AgentId)
	}
	if req.Request.Method != http.MethodPost {
		t.Fatalf("expected POST, got %s", req.Request.Method)
	}
}

// --- copyMetadataHeaderToResponse ---

func TestCopyMetadataHeaderToResponse_WithTimestamps(t *testing.T) {
	w := httptest.NewRecorder()
	timestamps := &pb.Timestamps{
		RequestAtHttpServer:  1000,
		RequestAtGrpcServer:  2000,
		RequestAtAgent:       3000,
		ResponseAtAgent:      4000,
		ResponseAtGrpcServer: 5000,
		ResponseAtHttpServer: 6000,
	}
	err := copyMetadataHeaderToResponse(w, "agentID-1", 100, 200, timestamps)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	h := w.Header().Get(session.MetadataHeader)
	if h == "" {
		t.Fatal("expected non-empty metadata header")
	}
}

func TestCopyMetadataHeaderToResponse_NilTimestamps(t *testing.T) {
	w := httptest.NewRecorder()
	err := copyMetadataHeaderToResponse(w, "agentID-1", 100, 200, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	h := w.Header().Get(session.MetadataHeader)
	if h == "" {
		t.Fatal("expected non-empty metadata header")
	}
}

// ===========================================================================

func TestProxyServeHTTP_Success(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "agentID-1")
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", strings.NewReader("body"))
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-success")
	w := httptest.NewRecorder()

	// Feed a response into the proxy's response channel after a short delay.
	// ServeHTTP will create the channel itself, so we inject via fanoutResponse.
	go func() {
		// Wait for ServeHTTP to register the response channel
		var ok bool
		for range 50 {
			time.Sleep(10 * time.Millisecond)
			ok = proxy.txnRegistry.IsRegistered("txn-success")
			if ok {
				break
			}
		}
		if !ok {
			return
		}
		response := &pb.HTTPProxyResponse{
			Response: &pb.HTTPResponse{
				TransactionId: "txn-success",
				StatusCode:    200,
				Payload:       []byte("hello"),
				Headers:       map[string]*pb.HeaderValueList{"Content-Type": {HeaderValues: []string{"text/plain"}}},
			},
			Timestamps: &pb.Timestamps{
				RequestAtHttpServer:  time.Now().UnixMilli(),
				RequestAtGrpcServer:  time.Now().UnixMilli(),
				RequestAtAgent:       time.Now().UnixMilli(),
				ResponseAtAgent:      time.Now().UnixMilli(),
				ResponseAtGrpcServer: time.Now().UnixMilli(),
				ResponseAtHttpServer: time.Now().UnixMilli(),
			},
			Routing: &pb.RoutingInfo{AgentId: "agentID-1"},
		}
		msg := &testBrokerMessage{raw: response}
		_ = proxy.fanoutResponse(msg, time.Now())
	}()

	proxy.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "hello") {
		t.Fatalf("expected 'hello' in body, got: %s", w.Body.String())
	}
}

// ===========================================================================
// Proxy.ServeHTTP: 0-byte payload (e.g. Content-Length: 0 from upstream)
// ===========================================================================

func TestProxyServeHTTP_EmptyPayload(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "agentID-1")
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/custom.css", nil)
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-empty")
	w := httptest.NewRecorder()

	go func() {
		var ok bool
		for range 50 {
			time.Sleep(10 * time.Millisecond)
			ok = proxy.txnRegistry.IsRegistered("txn-empty")
			if ok {
				break
			}
		}
		if !ok {
			return
		}
		response := &pb.HTTPProxyResponse{
			Response: &pb.HTTPResponse{
				TransactionId: "txn-empty",
				StatusCode:    200,
				// Payload is nil — simulates 0-byte body (e.g. empty CSS file)
				Headers: map[string]*pb.HeaderValueList{
					"Content-Type": {HeaderValues: []string{"text/css"}},
					"ETag":         {HeaderValues: []string{`"0-abc"`}},
				},
			},
			Timestamps: &pb.Timestamps{
				RequestAtHttpServer:  time.Now().UnixMilli(),
				RequestAtGrpcServer:  time.Now().UnixMilli(),
				RequestAtAgent:       time.Now().UnixMilli(),
				ResponseAtAgent:      time.Now().UnixMilli(),
				ResponseAtGrpcServer: time.Now().UnixMilli(),
				ResponseAtHttpServer: time.Now().UnixMilli(),
			},
			Routing: &pb.RoutingInfo{AgentId: "agentID-1"},
		}
		msg := &testBrokerMessage{raw: response}
		_ = proxy.fanoutResponse(msg, time.Now())
	}()

	proxy.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Fatalf("expected empty body, got %d bytes", w.Body.Len())
	}
	if cl := w.Header().Get("Content-Length"); cl != "0" {
		t.Fatalf("expected Content-Length: 0, got %q", cl)
	}
}

// ===========================================================================
// Proxy.ServeHTTP: publish error path
// ===========================================================================

func TestProxyServeHTTP_PublishError(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)
	// Close broker to cause publish errors
	b.Close()

	req := httptest.NewRequest("GET", "/resource", strings.NewReader("body"))
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-pub-err")
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, req)

	// Should get 503 Service Unavailable from publish error
	if w.Code != http.StatusServiceUnavailable && w.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 503 or 504, got %d", w.Code)
	}
}

// ===========================================================================
// FanoutResponses: abort on ack error
// ===========================================================================

// ===========================================================================
// RPCServer.getDispatcherURL: same env and cross env

// ===========================================================================

func TestPrepareAgentRequest_BodyNotRead(t *testing.T) {
	errBody := io.NopCloser(iotest.ErrReader(errors.New("read fail")))
	req := httptest.NewRequest("POST", "/test", errBody)
	// PrepareAgentRequest no longer reads the body, so it must succeed
	// even when the underlying reader would error.
	agentReq, body, err := prepareRequest(req, "agentID-1", "txn-1", time.Now().Add(time.Minute), time.Now())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	defer body.Close() //nolint:errcheck
	if agentReq.Request.Payload != nil {
		t.Fatal("expected nil payload")
	}
}

// ===========================================================================
// AgentTunnel: heartbeat enabled, heartbeat send failure

func TestProxyServeHTTP_MissingAgentID(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", nil)
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestProxyServeHTTP_MissingRequestAtProxy(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", nil)
	req.Header.Set(session.AgentIDKey, "agentID-1")
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestProxyServeHTTP_InvalidRequestAtProxy(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", nil)
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, "not-a-number")
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestProxyServeHTTP_MissingTransactionID(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", nil)
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestProxyServeHTTP_Timeout(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 100*time.Millisecond) // very short timeout
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "agentID-1")
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", strings.NewReader("body"))
	req.Header.Set(session.AgentIDKey, "agentID-1")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-1")
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	// Should timeout since no agent responds
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", w.Code)
	}
}

func TestProxy_publishRequest(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "agentID-1")
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	agentReq := &pb.HTTPProxyRequest{
		Request:    &pb.HTTPRequest{TransactionId: "txn-1"},
		Routing:    &pb.RoutingInfo{AgentId: "agentID-1"},
		Timestamps: &pb.Timestamps{},
	}

	err := proxy.publishRequest(context.Background(), "agentID-1", agentReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewProxy_ProducerEviction_OverflowCache(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 1)
	viper.Set("http.server.producers.cache_expiry", 50*time.Millisecond)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	p := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	// Create 2 topics and producers (cache size=1, second evicts first)
	topic1 := "agentID-evict-1"
	topic2 := "agentID-evict-2"
	_ = b.CreateTopic(topic1, 0, true)
	_ = b.CreateTopic(topic2, 0, true)

	if _, release, err := p.producers.Get("agentID-evict-1", b, broker.ProducerOptions{Topic: topic1}); err == nil {
		release()
	}
	if _, release, err := p.producers.Get("agentID-evict-2", b, broker.ProducerOptions{Topic: topic2}); err == nil { // evicts agentID-evict-1
		release()
	}

	time.Sleep(100 * time.Millisecond) // TTL eviction
}

func TestPublishRequest_SendError(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	// Broker without auto-creation and without the topic → CreateProducer fails
	b := broker.NewInMemoryMessageBroker()
	p := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	agentReq := &pb.HTTPProxyRequest{
		Request: &pb.HTTPRequest{
			TransactionId: "txn-pub-err",
		},
	}

	err := p.publishRequest(context.Background(), "agentID-pub-err", agentReq)
	if err == nil {
		t.Fatal("expected publishRequest error")
	}
	if !strings.Contains(err.Error(), "getting producer") {
		t.Fatalf("expected 'getting producer' error, got: %v", err)
	}
}

func TestProxy_ServeHTTP_BodyReadErrorStreaming(t *testing.T) {
	viper.Set("http.server.fanout_workers", 1)
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 200*time.Millisecond)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "oph-req-err")
	proxy := NewProxy(context.Background(), slog.New(slog.NewJSONHandler(io.Discard, nil)), b, testRelayCookie)

	// Body read error is now absorbed by ReadChunks (produces zero chunks);
	// the request is published with empty payload and times out with no agent.
	req := httptest.NewRequest("POST", "/test", &errorReader{err: fmt.Errorf("body read error")})
	req.Header.Set(session.AgentIDKey, "oph-req-err")
	req.Header.Set(session.TransactionIDHeader, "txn-req-err")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))

	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	// With no connected agent the request times out.
	if w.Code != http.StatusGatewayTimeout {
		t.Fatalf("expected 504, got %d", w.Code)
	}
}

func TestProxy_ServeHTTP_PublishError(t *testing.T) {
	viper.Set("http.server.fanout_workers", 1)
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	proxy := NewProxy(context.Background(), slog.New(slog.NewJSONHandler(io.Discard, nil)), b, testRelayCookie)

	// Inject a failing producer for the agentID so publishRequest fails on Send
	agentID := "oph-pub-err"
	proxy.producers.AddToCache(agentID, &errProducer{sendErr: fmt.Errorf("publish send failed")})

	req := httptest.NewRequest("POST", "/test", strings.NewReader("body"))
	req.Header.Set(session.AgentIDKey, agentID)
	req.Header.Set(session.TransactionIDHeader, "txn-pub-err")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))

	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

// TestProxyServeHTTP_ChunkReordering verifies that the proxy correctly
// reassembles a chunked HTTP response even when chunks arrive out of
// order in the per-transaction channel (as can happen with fanout_workers > 1).
func TestProxyServeHTTP_ChunkReordering(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "agentID-reorder")
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", nil)
	req.Header.Set(session.AgentIDKey, "agentID-reorder")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-reorder")
	w := httptest.NewRecorder()

	routing := &pb.RoutingInfo{AgentId: "agentID-reorder"}
	mkTS := func() *pb.Timestamps {
		now := time.Now().UnixMilli()
		return &pb.Timestamps{
			RequestAtHttpServer:  now,
			RequestAtGrpcServer:  now,
			RequestAtAgent:       now,
			ResponseAtAgent:      now,
			ResponseAtGrpcServer: now,
			ResponseAtHttpServer: now,
		}
	}

	// Build 3 chunks: chunk 0 has status+headers, chunks 1-2 have payload only.
	// Chunk 0: status 200, Content-Type header, payload "AAA"
	chunk0Headers := map[string]*pb.HeaderValueList{
		"Content-Type": {HeaderValues: []string{"text/plain"}},
	}
	chunking.SetChunkHeaders(chunk0Headers, 0, false)
	chunk0 := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{
			TransactionId: "txn-reorder",
			StatusCode:    200,
			Payload:       []byte("AAA"),
			Headers:       chunk0Headers,
		},
		Timestamps: mkTS(),
		Routing:    routing,
	}

	// Chunk 1: payload "BBB", no status code
	chunk1Headers := map[string]*pb.HeaderValueList{}
	chunking.SetChunkHeaders(chunk1Headers, 1, false)
	chunk1 := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{
			TransactionId: "txn-reorder",
			Payload:       []byte("BBB"),
			Headers:       chunk1Headers,
		},
		Timestamps: mkTS(),
		Routing:    routing,
	}

	// Chunk 2: payload "CCC", last chunk
	chunk2Headers := map[string]*pb.HeaderValueList{}
	chunking.SetChunkHeaders(chunk2Headers, 2, true)
	chunk2 := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{
			TransactionId: "txn-reorder",
			Payload:       []byte("CCC"),
			Headers:       chunk2Headers,
		},
		Timestamps: mkTS(),
		Routing:    routing,
	}

	go func() {
		// Wait for ServeHTTP to register the transaction.
		var ok bool
		for range 50 {
			time.Sleep(10 * time.Millisecond)
			ok = proxy.txnRegistry.IsRegistered("txn-reorder")
			if ok {
				break
			}
		}
		if !ok {
			return
		}

		// Inject chunks in REVERSE order: 2, 1, 0.
		// This simulates what happens when fanout_workers > 1.
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunk2}, time.Now())
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunk1}, time.Now())
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunk0}, time.Now())
	}()

	proxy.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	// Body must be in correct chunk order despite out-of-order delivery.
	want := "AAABBBCCC"
	if got := w.Body.String(); got != want {
		t.Fatalf("expected body %q, got %q", want, got)
	}
}

// TestProxyServeHTTP_ChunkReorderingLastArrivesMid verifies reordering when
// the last chunk (with X-Chunk-Last) arrives before an intermediate chunk.
func TestProxyServeHTTP_ChunkReorderingLastArrivesMid(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	viper.Set("default.request.timeout", 5*time.Second)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	_ = b.SetTopicAutoCreation("ns", true)
	standInAgent(t, b, "agentID-mid")
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	req := httptest.NewRequest("GET", "/resource", nil)
	req.Header.Set(session.AgentIDKey, "agentID-mid")
	req.Header.Set(session.RequestAtProxyHeader, fmt.Sprintf("%d", time.Now().UnixMilli()))
	req.Header.Set(session.TransactionIDHeader, "txn-mid-last")
	w := httptest.NewRecorder()

	routing := &pb.RoutingInfo{AgentId: "agentID-mid"}

	// 4 chunks: 0, 1, 2, 3(last)
	mkHeaders := func(idx int, last bool) map[string]*pb.HeaderValueList {
		h := map[string]*pb.HeaderValueList{}
		if idx == 0 {
			h["Content-Type"] = &pb.HeaderValueList{HeaderValues: []string{"application/octet-stream"}}
		}
		chunking.SetChunkHeaders(h, idx, last)
		return h
	}
	mkTS := func() *pb.Timestamps {
		now := time.Now().UnixMilli()
		return &pb.Timestamps{
			RequestAtHttpServer:  now,
			RequestAtGrpcServer:  now,
			RequestAtAgent:       now,
			ResponseAtAgent:      now,
			ResponseAtGrpcServer: now,
			ResponseAtHttpServer: now,
		}
	}

	chunks := []*pb.HTTPProxyResponse{
		{Response: &pb.HTTPResponse{TransactionId: "txn-mid-last", StatusCode: 200, Payload: []byte("00"), Headers: mkHeaders(0, false)}, Timestamps: mkTS(), Routing: routing},
		{Response: &pb.HTTPResponse{TransactionId: "txn-mid-last", Payload: []byte("11"), Headers: mkHeaders(1, false)}, Timestamps: mkTS(), Routing: routing},
		{Response: &pb.HTTPResponse{TransactionId: "txn-mid-last", Payload: []byte("22"), Headers: mkHeaders(2, false)}, Timestamps: mkTS(), Routing: routing},
		{Response: &pb.HTTPResponse{TransactionId: "txn-mid-last", Payload: []byte("33"), Headers: mkHeaders(3, true)}, Timestamps: mkTS(), Routing: routing},
	}

	go func() {
		var ok bool
		for range 50 {
			time.Sleep(10 * time.Millisecond)
			ok = proxy.txnRegistry.IsRegistered("txn-mid-last")
			if ok {
				break
			}
		}
		if !ok {
			return
		}

		// Deliver order: chunk 0, chunk 3(last), chunk 2, chunk 1
		// Chunk 0 arrives first (headers), then last chunk arrives early.
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunks[0]}, time.Now())
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunks[3]}, time.Now())
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunks[2]}, time.Now())
		_ = proxy.fanoutResponse(&testBrokerMessage{raw: chunks[1]}, time.Now())
	}()

	proxy.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}
	want := "00112233"
	if got := w.Body.String(); got != want {
		t.Fatalf("expected body %q, got %q", want, got)
	}
}

func TestNewProxy_ProducerEviction(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 1) // cache size = 1
	viper.Set("http.server.producers.cache_expiry", 50*time.Millisecond)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)

	if proxy == nil {
		t.Fatal("expected non-nil proxy")
	}
	// The proxy is valid and can be used
	if proxy.producers == nil {
		t.Fatal("expected non-nil producers")
	}
}

func TestPrepareAgentRequest_Validation(t *testing.T) {
	req, body, err := prepareRequest(
		makeHTTPRequest("/resource", "GET", "body-data"),
		"agentID-1",
		"txn-1",
		time.Now().Add(time.Minute),
		time.Now(),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer body.Close() //nolint:errcheck
	if req.Request.TransactionId != "txn-1" {
		t.Fatalf("expected txn-1, got %s", req.Request.TransactionId)
	}
}

func TestNewProxy(t *testing.T) {
	viper.Set("http.server.producers.cache_size", 10)
	viper.Set("http.server.producers.cache_expiry", time.Minute)
	defer viper.Reset()

	b := broker.NewInMemoryMessageBroker()
	proxy := NewProxy(context.Background(), testLogger(), b, testRelayCookie)
	if proxy == nil {
		t.Fatal("expected non-nil proxy")
	}
}

// TestMetadataAppendJSONIsMarshal: appendJSON writes json.Marshal's bytes,
// for agent IDs plain and with every byte JSON escapes.
func TestMetadataAppendJSONIsMarshal(t *testing.T) {
	for _, id := range []string{"", "agent-1", "agent_x.y:z@w+v~", `a"b`, `a\b`, "a<b>&c", "a\nb\x00\x1f\x7f", "agént", " ", "\xff"} {
		m := Metadata{AgentID: id, RequestAtHTTPServer: -1, RequestAtDispatcher: 1 << 62, RequestAtGRPCServer: 0,
			RequestAtAgent: 1760000000000, ResponseAtAgent: 42, ResponseAtGRPCServer: -9e15, ResponseAtDispatcher: 7, ResponseAtHTTPServer: 1}
		want, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		got, err := m.appendJSON(nil)
		if err != nil || string(got) != string(want) {
			t.Fatalf("agent %q: %s, %v; json.Marshal %s", id, got, err, want)
		}
	}
	h := headerOnly(http.Header{})
	if n := testing.AllocsPerRun(200, func() { _ = copyMetadataHeaderToResponse(h, "agent-1", 1, 2, nil) }); n > 2 {
		t.Fatalf("%v allocations per response header, budget 2 (the string and the header's slice)", n)
	}
}

// standInAgent subscribes to agentID's topic as the agent's session does,
// so the proxy's requests to it are taken (a send to a topic with no
// consumer fails).
func standInAgent(t *testing.T, b broker.MessageBroker, agentID string) {
	t.Helper()
	if err := b.CreateTopic(agentID, 0, true); err != nil {
		t.Fatal(err)
	}
	c, err := b.Subscribe(broker.ConsumerOptions{Topic: agentID, SubscriptionName: agentID})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
}
