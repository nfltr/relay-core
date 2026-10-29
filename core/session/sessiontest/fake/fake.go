// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package fake holds test doubles shared by the tests of core/session. It
// does not import core/session, so session's own in-package tests can use
// it (core/session/sessiontest imports session and cannot be imported by
// them).
package fake

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"runtime/metrics"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// MockAgentTunnelServer is an agent's tunnel server stream: Recv returns
// RecvMsgs in order (then io.EOF) or RecvErr, Send appends to Sent or fails
// with SendErr.
type MockAgentTunnelServer struct {
	Ctx      context.Context
	RecvMsgs []*pb.TunnelFrame
	RecvIdx  int
	RecvErr  error
	SendErr  error
	Sent     []*pb.TunnelFrame
}

// Send appends msg to Sent, or returns SendErr.
func (m *MockAgentTunnelServer) Send(msg *pb.TunnelFrame) error {
	if m.SendErr != nil {
		return m.SendErr
	}
	m.Sent = append(m.Sent, msg)
	return nil
}

// Recv returns the next of RecvMsgs, io.EOF after the last, or RecvErr.
func (m *MockAgentTunnelServer) Recv() (*pb.TunnelFrame, error) {
	if m.RecvErr != nil {
		return nil, m.RecvErr
	}
	if m.RecvIdx >= len(m.RecvMsgs) {
		return nil, io.EOF
	}
	msg := m.RecvMsgs[m.RecvIdx]
	m.RecvIdx++
	return msg, nil
}

// Context returns Ctx.
func (m *MockAgentTunnelServer) Context() context.Context { return m.Ctx }

// SendMsg does nothing.
func (m *MockAgentTunnelServer) SendMsg(_ any) error { return nil }

// RecvMsg does nothing.
func (m *MockAgentTunnelServer) RecvMsg(_ any) error { return nil }

// SetHeader does nothing.
func (m *MockAgentTunnelServer) SetHeader(_ metadata.MD) error { return nil }

// SendHeader does nothing.
func (m *MockAgentTunnelServer) SendHeader(_ metadata.MD) error { return nil }

// SetTrailer does nothing.
func (m *MockAgentTunnelServer) SetTrailer(_ metadata.MD) {}

// LockedBuffer is a concurrency-safe log sink: the A2A route logs from its
// own goroutines.
type LockedBuffer struct {
	Mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p.
func (b *LockedBuffer) Write(p []byte) (int, error) {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return b.buf.Write(p)
}

// Contains reports whether the buffer holds msg.
func (b *LockedBuffer) Contains(msg string) bool {
	b.Mu.Lock()
	defer b.Mu.Unlock()
	return bytes.Contains(b.buf.Bytes(), []byte(msg))
}

// Levels counts the buffer's JSON log lines by level, logging each line
// above debug.
func (b *LockedBuffer) Levels(t *testing.T) map[string]int {
	t.Helper()
	b.Mu.Lock()
	defer b.Mu.Unlock()
	counts := map[string]int{}
	dec := json.NewDecoder(bytes.NewReader(b.buf.Bytes()))
	for dec.More() {
		var rec struct {
			Level string `json:"level"`
			Msg   string `json:"msg"`
		}
		if err := dec.Decode(&rec); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		counts[rec.Level]++
		if rec.Level != "DEBUG" {
			t.Logf("non-debug log on the A2A success path: %s %q", rec.Level, rec.Msg)
		}
	}
	return counts
}

// TestProducer implements broker.Producer for testing.
type TestProducer struct {
	LastData any
	SendErr  error
}

// SendBytes keeps data as LastData and returns SendErr.
func (p *TestProducer) SendBytes(_ context.Context, data []byte) (string, error) {
	p.LastData = data
	return "msg-id", p.SendErr
}

// SendProto keeps msg as LastData and returns SendErr.
func (p *TestProducer) SendProto(_ context.Context, msg proto.Message) (string, error) {
	p.LastData = msg
	return "msg-id", p.SendErr
}

// Close does nothing.
func (p *TestProducer) Close() {}

// MockCache is a directory.Cache over Data that fails every call with Err
// when set and counts Set calls; its set operations do nothing.
type MockCache struct {
	Mu             sync.Mutex
	Data           map[string]*directory.Value
	Err            error
	SetCalls       int
	LastExpiration time.Duration
	Counters       map[string]int64
}

// Get returns key's record, nil when none.
func (m *MockCache) Get(_ context.Context, key string) (*directory.Value, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.Err != nil {
		return nil, m.Err
	}
	v, ok := m.Data[key]
	if !ok {
		return nil, nil
	}
	return v, nil
}

// Set counts the call, keeps exp as LastExpiration and stores value when it
// is a directory.Value.
func (m *MockCache) Set(_ context.Context, key string, value any, exp time.Duration) (string, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.Err != nil {
		return "", m.Err
	}
	m.SetCalls++
	m.LastExpiration = exp
	if v, ok := value.(directory.Value); ok {
		m.Data[key] = &v
	}
	return "OK", nil
}

// Del removes keys and returns how many existed.
func (m *MockCache) Del(_ context.Context, keys ...string) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.Err != nil {
		return 0, m.Err
	}
	var count int64
	for _, k := range keys {
		if _, ok := m.Data[k]; ok {
			delete(m.Data, k)
			count++
		}
	}
	return count, nil
}

// Incr adds by to key's counter and returns it.
func (m *MockCache) Incr(_ context.Context, key string, by int64, _ time.Duration) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	if m.Err != nil {
		return 0, m.Err
	}
	if m.Counters == nil {
		m.Counters = make(map[string]int64)
	}
	m.Counters[key] += by
	return m.Counters[key], nil
}

// Ping answers PONG and Err.
func (m *MockCache) Ping(_ context.Context) (string, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	return "PONG", m.Err
}

// SAdd does nothing.
func (m *MockCache) SAdd(_ context.Context, _ string, _ ...string) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	return 0, nil
}

// SRem does nothing.
func (m *MockCache) SRem(_ context.Context, _ string, _ ...string) (int64, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	return 0, nil
}

// SInter returns nothing.
func (m *MockCache) SInter(_ context.Context, _ ...string) ([]string, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	return nil, nil
}

// Snapshot returns the Set calls, the last expiration and a copy of Data.
func (m *MockCache) Snapshot() (int, time.Duration, map[string]*directory.Value) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	data := make(map[string]*directory.Value, len(m.Data))
	for key, value := range m.Data {
		if value == nil {
			data[key] = nil
			continue
		}
		clone := *value
		if value.Labels != nil {
			clone.Labels = cloneStringMap(value.Labels)
		}
		clone.Capabilities = cloneStringSlice(value.Capabilities)
		data[key] = &clone
	}
	return m.SetCalls, m.LastExpiration, data
}

// EndableStream is a ConnectTCP server stream that ends when End is closed.
type EndableStream struct {
	grpc.ServerStream
	End    chan struct{}
	Mu     sync.Mutex
	Header metadata.MD
	Ctx    context.Context // the agent's request metadata; nil: none
}

// SentHeader is the header the relay sent, waiting a moment for it: it
// goes out once the stream registered.
func (s *EndableStream) SentHeader() metadata.MD {
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(time.Millisecond) {
		s.Mu.Lock()
		h := s.Header
		s.Mu.Unlock()
		if h != nil || time.Now().After(deadline) {
			return h
		}
	}
}

// Recv waits for End to close, then returns io.EOF.
func (s *EndableStream) Recv() (*pb.TCPData, error) { <-s.End; return nil, io.EOF }

// Send discards the frame.
func (s *EndableStream) Send(*pb.TCPData) error { return nil }

// SendHeader keeps md as Header.
func (s *EndableStream) SendHeader(md metadata.MD) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Header = md
	return nil
}

// SetHeader does nothing.
func (s *EndableStream) SetHeader(metadata.MD) error { return nil }

// SetTrailer does nothing.
func (s *EndableStream) SetTrailer(metadata.MD) {}

// SendMsg does nothing.
func (s *EndableStream) SendMsg(any) error { return nil }

// RecvMsg does nothing.
func (s *EndableStream) RecvMsg(any) error { return nil }

// Context: RegisterStream reads the agent's request metadata. allow:wrapper
func (s *EndableStream) Context() context.Context {
	if s.Ctx != nil {
		return s.Ctx
	}
	return context.Background()
}

// PlainSession is a loopback TCP connection as a peerlink.Session: a peer
// that negotiated no link.FeatureChain.
type PlainSession struct{ *net.TCPConn }

// Reset closes the connection with an RST.
func (s PlainSession) Reset() error { _ = s.SetLinger(0); return s.Close() }

// PlainSessionPair is a session's accepting end (as a peerlink.Session) and
// its opener's.
func PlainSessionPair(t *testing.T) (PlainSession, *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	a, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close(); _ = a.Close() })
	return PlainSession{a.(*net.TCPConn)}, c.(*net.TCPConn)
}

// cloneStringSlice copies values; empty is nil (an agent's advertised
// capabilities and routes as AgentInfo carries them).
func cloneStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

// cloneStringMap copies values; empty is nil.
func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	maps.Copy(cloned, values)
	return cloned
}

// TestLogger is a JSON logger that discards its output.
func TestLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// GoroutineProfiles writes the goroutine (debug=2) and goroutine leak
// (debug=1) profiles of the running rig to RELAY_PERF_PROFILE_DIR (scripts/perf.sh
// profile), named after the test; nothing when it is unset.
func GoroutineProfiles(tb testing.TB) {
	dir := os.Getenv("RELAY_PERF_PROFILE_DIR")
	if dir == "" {
		return
	}
	name := strings.NewReplacer("/", "_", "=", "-").Replace(tb.Name())
	for _, p := range []string{"goroutine", "goroutineleak"} {
		prof := pprof.Lookup(p)
		if prof == nil {
			continue
		}
		f, err := os.Create(filepath.Join(dir, name+"."+p+".txt")) // #nosec G304 G703 -- the operator's directory (RELAY_PERF_PROFILE_DIR), test rig
		if err != nil {
			tb.Error(err)
			return
		}
		debug := 2 // every goroutine's stack
		if p == "goroutineleak" {
			debug = 1 // "goroutineleak profile: total N": the leaked ones only
		}
		_ = prof.WriteTo(f, debug)
		_ = f.Close()
	}
}

// SchedCounts reads the scheduler's run transitions (wakeups: the
// runtime samples 1 in 8 into /sched/latencies:seconds, so its count x8
// estimates them) and the goroutines created since the program started.
func SchedCounts() (wakeups, created float64) {
	s := []metrics.Sample{{Name: "/sched/latencies:seconds"}, {Name: "/sched/goroutines-created:goroutines"}}
	metrics.Read(s)
	var n uint64
	for _, c := range s[0].Value.Float64Histogram().Counts {
		n += c
	}
	return float64(n) * 8, float64(s[1].Value.Uint64())
}

// ReportSched reports wakeups/op and goroutines/op since w0, c0
// (SchedCounts before the loop).
func ReportSched(b *testing.B, w0, c0 float64) {
	w, c := SchedCounts()
	b.ReportMetric((w-w0)/float64(b.N), "wakeups/op")
	b.ReportMetric((c-c0)/float64(b.N), "goroutines/op")
}

// StaticAddr is a listener that is only an address: a harness whose
// visitors dial the target itself (the bare-socket floor).
type StaticAddr struct {
	net.Listener
	A net.Addr
}

// Addr returns A.
func (l StaticAddr) Addr() net.Addr { return l.A }
