// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"errors"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/nfltr/relay-core/core/directory"
)

// moveSink records what the relay sends an agent.
type moveSink struct {
	mu   sync.Mutex
	sent []pb.Move
}

func (s *moveSink) Send(r *pb.HTTPProxyRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, r.GetMove())
	return nil
}
func (s *moveSink) Recv() (*pb.HTTPProxyResponse, error) { select {} }
func (s *moveSink) steps() []pb.Move {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pb.Move(nil), s.sent...)
}

func waitMove(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !ok(); time.Sleep(5 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 5 s", what)
		}
	}
}

// movingSession is an HTTP session of an agent that moves, registered on a
// draining-capable replica (pod dialer, shared inventory) whose record names
// it; drain starts the replica's drain.
func movingSession(t *testing.T) (d *Dispatcher, s *HTTPStream, sink *moveSink, mc *fake.MockCache, inv directory.IPInventory, drain func()) {
	t.Helper()
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	global, drain := context.WithCancel(context.Background())
	t.Cleanup(drain)
	d, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	d.globalContext, d.gracefulShutdownTimeout, d.PodDialer = global, 10*time.Millisecond, staticRefusingDialer{}
	mc = newMockCache()
	inv, _ = directory.NewIPInventory(fake.TestLogger(), mc, time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	sink = &moveSink{}
	ticker := time.NewTicker(time.Hour)
	t.Cleanup(ticker.Stop)
	s = NewHTTPStream("agent-1", sink, t.Context(), make(chan proto.Message), ticker)
	s.connectedAt = time.Now().Add(-time.Minute)
	s.moved = make(chan struct{})
	d.Agents.Store("agent-1", s)
	mc.Data[directory.GetDispatcherCacheKey("agent-1")] = &directory.Value{PodIP: "10.0.0.1", Since: s.connectedAt.UnixNano()}
	return d, s, sink, mc, inv, drain
}

// A draining replica asks an agent that moves to connect again instead of
// letting it go; once the agent reports its new connection and its record
// names the new session, the replica stops routing to the old stream, then
// tells the agent no request follows (MOVE_DONE), and the session ends when
// the agent closes the stream.
func TestAgentMovesBeforeTheReplicaLetsItGo(t *testing.T) {
	d, s, sink, mc, inv, drain := movingSession(t)
	received := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- d.WatchAgentSession(t.Context(), "agent-1", s, inv, received) }()

	drain()
	waitMove(t, "MOVE_RECONNECT", func() bool { return len(sink.steps()) == 1 && sink.steps()[0] == pb.Move_MOVE_RECONNECT })
	if !d.IsAgentConnected("agent-1") {
		t.Fatal("the replica stopped routing to the agent before its new connection was up")
	}

	// The agent's new connection, on another replica, took its record.
	mc.Mu.Lock()
	mc.Data[directory.GetDispatcherCacheKey("agent-1")] = &directory.Value{PodIP: "10.0.0.2", Since: time.Now().UnixNano()}
	mc.Mu.Unlock()
	d.processHTTPInput(t.Context(), s, &pb.HTTPProxyResponse{Move: pb.Move_MOVE_CONNECTED}, time.Now()) //nolint:errcheck
	waitMove(t, "routing stopped", func() bool { return !d.IsAgentConnected("agent-1") })
	if steps := sink.steps(); len(steps) != 1 {
		t.Fatalf("MOVE_DONE before the queued requests settled: %v", steps)
	}
	waitMove(t, "MOVE_DONE", func() bool { return len(sink.steps()) == 2 && sink.steps()[1] == pb.Move_MOVE_DONE })
	select {
	case err := <-done:
		t.Fatalf("session ended (%v) before the agent closed the stream", err)
	case <-time.After(100 * time.Millisecond):
	}
	received <- nil // the agent closed the stream
	if err := <-done; err != nil {
		t.Fatalf("session ended with %v", err)
	}
}

// An agent that does not report a new connection within its moveBound is let
// go as before.
func TestAgentThatDoesNotMoveIsLetGo(t *testing.T) {
	d, s, sink, _, inv, drain := movingSession(t)
	d.moveBound = 100 * time.Millisecond
	done := make(chan error, 1)
	go func() { done <- d.WatchAgentSession(t.Context(), "agent-1", s, inv, nil) }()
	drain()
	select {
	case err := <-done:
		if !errors.Is(err, errServerRestarting) || len(sink.steps()) != 1 {
			t.Fatalf("ended with %v after %v, want errServerRestarting after MOVE_RECONNECT", err, sink.steps())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never let go")
	}
}

// An agent that predates the move, or a replica without peers, gets no
// move step: it is let go as before.
func TestNoMoveWithoutTheAgentOrPeers(t *testing.T) {
	for name, setup := range map[string]func(*Dispatcher, *HTTPStream){
		"agent predates it": func(_ *Dispatcher, s *HTTPStream) { s.moved = nil },
		"single replica":    func(d *Dispatcher, _ *HTTPStream) { d.PodDialer = nil },
	} {
		t.Run(name, func(t *testing.T) {
			d, s, sink, _, inv, drain := movingSession(t)
			setup(d, s)
			drain()
			if err := d.WatchAgentSession(t.Context(), "agent-1", s, inv, nil); !errors.Is(err, errServerRestarting) || len(sink.steps()) != 0 {
				t.Fatalf("ended with %v, sent %v; want errServerRestarting and no step", err, sink.steps())
			}
		})
	}
}

// A leaving replica with peers refuses new agent sessions, so an agent's
// reconnect reaches another replica.
func TestLeavingReplicaRefusesNewAgents(t *testing.T) {
	d, _, _, _, _, drain := movingSession(t)
	drain()
	err := d.ConnectHTTP(&mockConnectHTTPServer{ctx: makeGRPCContext("agent-2")})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("a leaving replica admitted an agent: %v", err)
	}
}

// The TCP stream of an agent that moves is not let go during a drain: the
// agent closes it with its old connection.
func TestConnectTCPOfAMovingAgentStaysWhileDraining(t *testing.T) {
	global, drain := context.WithCancel(context.Background())
	const window = 200 * time.Millisecond
	d := &Dispatcher{TCPDispatcher: NewTCPDispatcher(slog.New(slog.DiscardHandler), staticRefusingDialer{}, nil), Logger: slog.New(slog.DiscardHandler)}
	d.TCPDispatcher.ConfigureDrain(global.Done(), window)
	ctx := testPrincipal(metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-apikey-agentID", "mover", "x-auth-method", "apikey", agentwire.MoveKey, "1")))
	st := &fake.EndableStream{End: make(chan struct{}), Ctx: ctx}
	done := make(chan error, 1)
	go func() { done <- d.ConnectTCP(st) }()
	waitStreamsOf(t, d.TCPDispatcher, "mover", 1)
	drain()
	select {
	case err := <-done:
		t.Fatalf("a moving agent's TCP stream was let go: %v", err)
	case <-time.After(window):
	}
	close(st.End) // the agent closes it
	<-done
}

// Clock skew: the new session's replica runs behind, so its record's start
// time is older than the old session's. The agent still moves, and the old
// stream's inbound frames (answers to what it still owes) do not take the
// record back.
func TestAgentMovesUnderClockSkew(t *testing.T) {
	d, s, sink, mc, inv, drain := movingSession(t)
	received := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- d.WatchAgentSession(t.Context(), "agent-1", s, inv, received) }()
	drain()
	waitMove(t, "MOVE_RECONNECT", func() bool { return len(sink.steps()) == 1 })
	key := directory.GetDispatcherCacheKey("agent-1")
	behind := &directory.Value{PodIP: "10.0.0.2", Since: s.connectedAt.Add(-5 * time.Minute).UnixNano()}
	mc.Mu.Lock()
	mc.Data[key] = behind
	mc.Mu.Unlock()
	d.InventoryRefreshByAgentID.Delete("agent-1")                                                                                                        // a renewal is due
	d.processHTTPInput(t.Context(), s, &pb.HTTPProxyResponse{Move: pb.Move_MOVE_CONNECTED}, time.Now())                                                  //nolint:errcheck
	d.processHTTPInput(t.Context(), s, &pb.HTTPProxyResponse{Response: &pb.HTTPResponse{TransactionId: "t1"}, Timestamps: &pb.Timestamps{}}, time.Now()) //nolint:errcheck
	waitMove(t, "MOVE_DONE", func() bool { return len(sink.steps()) == 2 && sink.steps()[1] == pb.Move_MOVE_DONE })
	mc.Mu.Lock()
	got := mc.Data[key]
	mc.Mu.Unlock()
	if got != behind {
		t.Fatalf("the old stream took the record back: %+v", got)
	}
	received <- nil
	<-done
}
