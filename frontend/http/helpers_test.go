// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/keyfile"

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/core/session"
	"github.com/nfltr/relay-core/grpcfactory"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type errProducer struct {
	sendErr error
}

// mockConsumer implements broker.Consumer for testing.
type mockConsumer struct {
	ch       chan broker.Message
	unsubErr error
}

// stubMessage is a simple broker.Message for tests.
type stubMessage struct {
	payload []byte
	ackErr  error
}

type errorReader struct {
	err error
}

// errorInventory implements IPInventory and always returns an error
type errorInventory struct {
	err error
}

func (e *errorInventory) GetIP(ctx context.Context, agentID string) (string, error) {
	return "", e.err
}

func (e *errorInventory) ListAgentIDs(ctx context.Context) ([]string, error) {
	return nil, e.err
}

func (e *errorInventory) ListAgentRecords(ctx context.Context) ([]directory.AgentRecord, error) {
	return nil, e.err
}

func (e *errorInventory) SetIP(ctx context.Context, agentID string, expiration time.Duration) error {
	return e.err
}

func (e *errorInventory) SetIPWithLabels(_ context.Context, _ string, _ map[string]string, _ time.Duration) error {
	return e.err
}

func (e *errorInventory) SetIPWithAgentInfo(_ context.Context, _ string, _ map[string]string, _ []string, _ time.Duration) error {
	return e.err
}

func (e *errorInventory) DeleteIPs(ctx context.Context, agentIDs ...string) error {
	return e.err
}

type mockHTTPStreamer struct {
	sendErr error
	recvMsg *pb.HTTPProxyResponse
	recvErr error
}

func (m *mockHTTPStreamer) Send(_ *pb.HTTPProxyRequest) error { return m.sendErr }

func (m *mockHTTPStreamer) Recv() (*pb.HTTPProxyResponse, error) { return m.recvMsg, m.recvErr }

func (p *errProducer) SendBytes(_ context.Context, _ []byte) (string, error) {
	return "", p.sendErr
}
func (p *errProducer) SendProto(_ context.Context, _ proto.Message) (string, error) {
	return "", p.sendErr
}
func (p *errProducer) Close()                        {}
func (m *mockConsumer) Ack(msg broker.Message) error { return msg.Ack() }
func (m *mockConsumer) Chan() <-chan broker.Message  { return m.ch }
func (m *mockConsumer) Close()                       {}
func (m *mockConsumer) Unsubscribe() error           { return m.unsubErr }
func (s *stubMessage) Payload() []byte               { return s.payload }
func (s *stubMessage) RawObject() proto.Message      { return nil }
func (s *stubMessage) Ack() error                    { return s.ackErr }
func (e *errorReader) Read(_ []byte) (int, error) {
	return 0, e.err
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// testBrokerMessage implements broker.Message for testing.
type testBrokerMessage struct {
	payload []byte
	raw     proto.Message
}

func (m *testBrokerMessage) Payload() []byte { return m.payload }

func (m *testBrokerMessage) RawObject() proto.Message { return m.raw }

func (m *testBrokerMessage) Ack() error { return nil }

// testProducer implements broker.Producer for testing.
type testProducer struct {
	lastData any
	sendErr  error
}

func (p *testProducer) SendBytes(_ context.Context, data []byte) (string, error) {
	p.lastData = data
	return "msg-id", p.sendErr
}

func (p *testProducer) SendProto(_ context.Context, msg proto.Message) (string, error) {
	p.lastData = msg
	return "msg-id", p.sendErr
}

func (p *testProducer) Close() {}

func newMockCache() *mockCache {
	return &mockCache{data: make(map[string]*directory.Value)}
}

func makeHTTPRequest(path, method, body string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	return r
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

// waitAgentInfo returns agentID's inventory entry once list has it.
func waitAgentInfo(t *testing.T, list func() []session.AgentInfo, agentID string) session.AgentInfo {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, info := range list() {
			if info.AgentID == agentID {
				return info
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never registered", agentID)
	return session.AgentInfo{}
}

// serveDispatcherAs serves d on a loopback listener, every stream
// authenticated as agentID.
func serveDispatcherAs(t *testing.T, d *session.Dispatcher, agentID string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// WaitForHandlers: Stop returns once the agent's stream handler did,
	// which reads viper (the next test sets it).
	srv := grpc.NewServer(grpcfactory.ProtocolOptions(0)[1], grpc.WaitForHandlers(true), grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		md, _ := metadata.FromIncomingContext(ss.Context())
		md = md.Copy()
		md.Set("x-apikey-agentID", agentID)
		return h(srv, identityStream{ss, testPrincipal(metadata.NewIncomingContext(ss.Context(), md))})
	}))
	pb.RegisterDispatcherServer(srv, d)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

// testRelayCookie names the relay's cookies as the relay does
// (relay.relayCookieNames): each bare and with the __Host- prefix.
func testRelayCookie(name string) bool {
	switch strings.TrimPrefix(name, "__Host-") {
	case "rpc_session", "rpc_browse", "cli_signup", "oauth_state", "oauth_pkce":
		return true
	}
	return false
}

// testHost is the relay as HTTP ingress sees it; methods the ingress does
// not call are left nil (frontend.Host).
type testHost struct {
	frontend.Host
	core       *session.Core
	registered func(ctx context.Context, agentID string) bool
	mounted    frontend.AgentHTTP
}

func (h *testHost) Core() *session.Core            { return h.core }
func (h *testHost) Logger() *slog.Logger           { return testLogger() }
func (h *testHost) RelayCookie(name string) bool   { return testRelayCookie(name) }
func (h *testHost) MountHTTP(a frontend.AgentHTTP) { h.mounted = a }
func (h *testHost) Registered(ctx context.Context, agentID string) bool {
	return h.registered != nil && h.registered(ctx, agentID)
}

// newTestIngress is the ingress over c (its dispatchers as the test wires
// them, also later: the ingress reads them per request).
func newTestIngress(c *session.Core) *Ingress {
	if c.Broker == nil {
		c.Broker = broker.NewInMemoryMessageBroker()
	}
	return New(context.Background(), &testHost{core: c})
}

// prepareRequest is prepareAgentRequest of a proxy that knows the relay's
// cookies.
func prepareRequest(r *http.Request, agentID, transactionID string, deadline, at time.Time) (*pb.HTTPProxyRequest, io.ReadCloser, error) {
	return (&Proxy{relayCookie: testRelayCookie}).prepareAgentRequest(r, agentID, transactionID, deadline, at)
}

type mockCache struct {
	mu             sync.Mutex
	data           map[string]*directory.Value
	err            error
	setCalls       int
	lastExpiration time.Duration
	counters       map[string]int64
}

func (m *mockCache) Get(_ context.Context, key string) (*directory.Value, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	v, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (m *mockCache) Set(_ context.Context, key string, value any, exp time.Duration) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return "", m.err
	}
	m.setCalls++
	m.lastExpiration = exp
	if v, ok := value.(directory.Value); ok {
		m.data[key] = &v
	}
	return "OK", nil
}

func (m *mockCache) Del(_ context.Context, keys ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, m.err
	}
	var count int64
	for _, k := range keys {
		if _, ok := m.data[k]; ok {
			delete(m.data, k)
			count++
		}
	}
	return count, nil
}

func (m *mockCache) Incr(_ context.Context, key string, by int64, _ time.Duration) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, m.err
	}
	if m.counters == nil {
		m.counters = make(map[string]int64)
	}
	m.counters[key] += by
	return m.counters[key], nil
}

func (m *mockCache) Ping(_ context.Context) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return "PONG", m.err
}

func (m *mockCache) SAdd(_ context.Context, _ string, _ ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, nil
}

func (m *mockCache) SRem(_ context.Context, _ string, _ ...string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return 0, nil
}

func (m *mockCache) SInter(_ context.Context, _ ...string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return nil, nil
}

// identityStream asserts agentID as the stream's authenticated principal,
// as the API-key interceptor does in production.
type identityStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s identityStream) Context() context.Context { return s.ctx }

// testPrincipal stands in for the key interceptor in tests that are not
// about authentication: the identity a test names in metadata
// ("x-apikey-agentID", "x-apikey-base-id") becomes the principal the
// interceptor would put into the context. Production never reads them.
func testPrincipal(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	id := session.FirstMD(md, "x-apikey-agentid")
	if id == "" {
		return ctx
	}
	base := session.FirstMD(md, "x-apikey-base-id")
	owner := base
	if owner == "" {
		owner = id
	}
	return identity.WithPrincipal(ctx, identity.Principal{Provider: keyfile.ProviderName, Owner: "keys:" + owner, Subject: base, AgentID: id})
}

// harnessAuxIdle, when set, is the harness relay's auxiliary idle time
// (read when a harness starts, so no running relay sees it change).
var harnessAuxIdle time.Duration
