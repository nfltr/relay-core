// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// --- SyncMap tests ---

func TestSyncMap_StoreAndLoad(t *testing.T) {
	var m SyncMap[string, int]
	m.Store("a", 1)

	v, ok := m.Load("a")
	if !ok || v != 1 {
		t.Fatalf("expected (1, true), got (%d, %v)", v, ok)
	}
}

func TestSyncMap_Load_Missing(t *testing.T) {
	var m SyncMap[string, int]
	v, ok := m.Load("missing")
	if ok {
		t.Fatal("expected false for missing key")
	}
	if v != 0 {
		t.Fatalf("expected zero value, got %d", v)
	}
}

func TestSyncMap_Delete(t *testing.T) {
	var m SyncMap[string, int]
	m.Store("a", 1)
	m.Delete("a")
	_, ok := m.Load("a")
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestSyncMap_Range(t *testing.T) {
	var m SyncMap[string, int]
	m.Store("a", 1)
	m.Store("b", 2)

	seen := make(map[string]int)
	m.Range(func(key string, value int) bool {
		seen[key] = value
		return true
	})

	if len(seen) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(seen))
	}
	if seen["a"] != 1 || seen["b"] != 2 {
		t.Fatalf("unexpected values: %v", seen)
	}
}

func TestSyncMap_Range_StopEarly(t *testing.T) {
	var m SyncMap[string, int]
	m.Store("a", 1)
	m.Store("b", 2)
	m.Store("c", 3)

	count := 0
	m.Range(func(key string, value int) bool {
		count++
		return false // stop after first
	})
	if count != 1 {
		t.Fatalf("expected 1 iteration, got %d", count)
	}
}

// --- Cast tests ---

func TestCast_OK(t *testing.T) {
	var x any = "hello"
	v, ok := Cast[string](x, true)
	if !ok || v != "hello" {
		t.Fatalf("expected (hello, true), got (%s, %v)", v, ok)
	}
}

func TestCast_NotOK(t *testing.T) {
	var x any = "hello"
	v, ok := Cast[string](x, false)
	if ok {
		t.Fatal("expected false")
	}
	if v != "" {
		t.Fatalf("expected zero value, got %s", v)
	}
}

// --- WorkerPool tests ---

func TestWorkerPool_Run_Success(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	ch <- 3
	close(ch)

	var sum atomic.Int64
	pool := NewWorkerPool(ch, 2, func(ctx context.Context, v int) error {
		sum.Add(int64(v))
		return nil
	})

	err := pool.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum.Load() != 6 {
		t.Fatalf("expected sum 6, got %d", sum.Load())
	}
}

func TestWorkerPool_Run_Error(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 1
	ch <- 2
	close(ch)

	pool := NewWorkerPool(ch, 1, func(ctx context.Context, v int) error {
		return errors.New("fail")
	})

	err := pool.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWorkerPool_Run_ContextCancel(t *testing.T) {
	ch := make(chan int)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool := NewWorkerPool(ch, 1, func(ctx context.Context, v int) error {
		return nil
	})

	err := pool.Run(ctx)
	if err == nil {
		t.Fatal("expected error from canceled context")
	}
}

func TestWorkerPool_Run_NoConcurrency(t *testing.T) {
	ch := make(chan int, 2)
	ch <- 10
	ch <- 20
	close(ch)

	sum := 0
	pool := NewWorkerPool(ch, 0, func(ctx context.Context, v int) error {
		sum += v
		return nil
	})

	err := pool.Run(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sum != 30 {
		t.Fatalf("expected sum 30, got %d", sum)
	}
}

// --- Remaining util.go gaps ---

func TestNewLogger_ValidLevel(t *testing.T) {
	logger := NewLogger("debug")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestNewLogger_InvalidLevel(t *testing.T) {
	logger := NewLogger("bogus")
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
}

func TestGetListeners(t *testing.T) {
	listeners, err := GetListeners([]string{"0"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
}

func TestGetListeners_InvalidPort(t *testing.T) {
	listeners, err := GetListeners([]string{"99999999"}, nil)
	if err == nil {
		t.Fatal("expected error for invalid port")
	}
	if listeners != nil {
		t.Fatal("expected nil map on error")
	}
}

func TestGetListeners_DuplicatePorts(t *testing.T) {
	listeners, err := GetListeners([]string{"0", "0"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Dedup means only one listener despite two "0" entries.
	if len(listeners) != 1 {
		t.Fatalf("expected 1 listener, got %d", len(listeners))
	}
	for _, l := range listeners {
		_ = l.Close()
	}
}

func TestGetListeners_CleanupOnError(t *testing.T) {
	// Open a valid port, then request an invalid one — the valid listener must be cleaned up.
	listeners, err := GetListeners([]string{"0", "99999999"}, nil)
	if err == nil {
		t.Fatal("expected error for invalid port")
	}
	if listeners != nil {
		t.Fatal("expected nil map on error")
	}
}

func TestIsPortListening_Free(t *testing.T) {
	// No listener on a random high port, should return false (not listening)
	result := IsPortListening("48999")
	if result {
		t.Fatal("expected port 48999 to not be listening")
	}
}

func TestIsPortListening_InUse(t *testing.T) {
	// Start a listener on a random port.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer srv.Close()
	// Extract port from srv.URL.
	port := srv.URL[len("http://127.0.0.1:"):]
	result := IsPortListening(port)
	if !result {
		t.Fatal("expected port to be listening")
	}
}

// --- ResponseWriter additional tests ---

func TestResponseWriter_GetStatusCode(t *testing.T) {
	rw := NewResponseWriter(httptest.NewRecorder(), nil)
	rw.WriteHeader(http.StatusNotFound)
	if rw.GetStatusCode() != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rw.GetStatusCode())
	}
}

func TestResponseWriter_GetSize(t *testing.T) {
	rw := NewResponseWriter(httptest.NewRecorder(), nil)
	n, err := rw.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes written, got %d", n)
	}
	if rw.GetSize() != 5 {
		t.Fatalf("expected size 5, got %d", rw.GetSize())
	}
}

func TestResponseWriter_WriteHeader_Idempotent(t *testing.T) {
	rw := NewResponseWriter(httptest.NewRecorder(), nil)
	rw.WriteHeader(http.StatusOK)
	rw.WriteHeader(http.StatusNotFound) // should be ignored
	if rw.GetStatusCode() != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.GetStatusCode())
	}
}

// --- CopyProtoHeadersToHTTPHeaders ---

func TestCopyProtoHeadersToHTTPHeaders(t *testing.T) {
	protoHeaders := map[string]*pb.HeaderValueList{
		"X-Custom":     {HeaderValues: []string{"v1", "v2"}},
		"Content-Type": {HeaderValues: []string{"application/json"}},
	}
	httpHeader := http.Header{}
	CopyProtoHeadersToHTTPHeaders(protoHeaders, httpHeader)

	if got := httpHeader.Values("X-Custom"); len(got) != 2 || got[0] != "v1" || got[1] != "v2" {
		t.Fatalf("expected [v1, v2], got %v", got)
	}
	if got := httpHeader.Get("Content-Type"); got != "application/json" {
		t.Fatalf("expected application/json, got %s", got)
	}
}

func TestCopyProtoHeadersToHTTPHeaders_Empty(t *testing.T) {
	httpHeader := http.Header{}
	CopyProtoHeadersToHTTPHeaders(nil, httpHeader)
	if len(httpHeader) != 0 {
		t.Fatalf("expected empty headers, got %v", httpHeader)
	}
}

// --- ProtoToMessage / BytesToMessage ---

func TestBytesToMessage(t *testing.T) {
	msg := BytesToTunnelFrame([]byte("payload"), "test-type", "agentID-1", true)
	if msg == nil {
		t.Fatal("expected non-nil message")
	}
	if string(msg.Payload) != "payload" {
		t.Fatalf("expected 'payload', got %q", msg.Payload)
	}
	if msg.MessageType != "test-type" {
		t.Fatalf("expected 'test-type', got %s", msg.MessageType)
	}
	if msg.Routing.AgentId != "agentID-1" {
		t.Fatalf("expected 'agentID-1', got %s", msg.Routing.AgentId)
	}
	if !msg.Eom {
		t.Fatal("expected Eom=true")
	}
}

func TestBytesToMessage_NoEom(t *testing.T) {
	msg := BytesToTunnelFrame([]byte("data"), "type-2", "agentID-2", false)
	if msg.Eom {
		t.Fatal("expected Eom=false")
	}
}

func TestProtoToMessage_Success(t *testing.T) {
	innerMsg := &pb.TunnelFrame{Payload: []byte("inner"), MessageType: "test"}
	msg, err := ProtoToTunnelFrame(innerMsg, "outer-type", "agentID-3", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg == nil {
		t.Fatal("expected non-nil message")
	}
	if msg.Routing.AgentId != "agentID-3" {
		t.Fatalf("expected agentID-3, got %s", msg.Routing.AgentId)
	}
}

// --- NewLogger for all known levels ---

func TestNewLogger_AllLevels(t *testing.T) {
	levels := []string{"debug", "info", "warn", "error"}
	for _, lvl := range levels {
		logger := NewLogger(lvl)
		if logger == nil {
			t.Fatalf("expected non-nil logger for level %s", lvl)
		}
	}
}

// --- ProtoToMessage marshal error path ---

func TestProtoToMessage_MarshalError(t *testing.T) {
	origMarshal := protoMarshal
	defer func() { protoMarshal = origMarshal }()

	protoMarshal = func(m proto.Message) ([]byte, error) {
		return nil, errors.New("forced marshal error")
	}

	_, err := ProtoToTunnelFrame(&pb.TunnelFrame{Payload: []byte("x")}, "t", "o", true)
	if err == nil || err.Error() != "forced marshal error" {
		t.Fatalf("expected forced marshal error, got %v", err)
	}
}

func TestGetListeners_BindsTheRequestedHost(t *testing.T) {
	listeners, err := GetListeners([]string{"0"}, func(string) string { return "127.0.0.1" })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, l := range listeners {
		if host, _, _ := net.SplitHostPort(l.Addr().String()); host != "127.0.0.1" {
			t.Fatalf("listener bound %s, want 127.0.0.1", l.Addr())
		}
		_ = l.Close()
	}
}
