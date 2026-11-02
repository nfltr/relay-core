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

	"github.com/nfltr/relay-core/broker"
	"github.com/nfltr/relay-core/core/directory"
	"github.com/nfltr/relay-core/internal/tracehook"
)

// moveSink records what the relay sends an agent.
type moveSink struct {
	mu     sync.Mutex
	sent   []pb.Move
	before []int // the requests sent before each move step
	reqs   int
}

func (s *moveSink) Send(r *pb.HTTPProxyRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case r.GetRequest() != nil:
		s.reqs++
	case r.GetMove() != pb.Move_MOVE_UNSPECIFIED:
		s.sent = append(s.sent, r.GetMove())
		s.before = append(s.before, s.reqs)
	}
	return nil
}
func (s *moveSink) Recv() (*pb.HTTPProxyResponse, error) { select {} }
func (s *moveSink) steps() []pb.Move {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pb.Move(nil), s.sent...)
}

// serveTopic runs the session's consumer of its agent's topic
// (relayRequestsToAgent), as ConnectHTTP does: it sends the move's
// MOVE_DONE.
func serveTopic(t *testing.T, d *Dispatcher, s *HTTPStream) {
	t.Helper()
	if err := d.broker.CreateTopic(s.ID, 0, true); err != nil {
		t.Fatal(err)
	}
	c, err := d.broker.Subscribe(broker.ConsumerOptions{Topic: s.ID, SubscriptionName: s.ID, Type: broker.ConsumerTypeExclusive})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = d.relayRequestsToAgent(ctx, s, c) }()
	t.Cleanup(func() { cancel(); <-done; c.Close() })
}

// publishTo publishes a request's first message to the agent's topic, as
// the ingress's proxy does.
func publishTo(t *testing.T, d *Dispatcher, agentID, txn string) {
	t.Helper()
	p, err := d.broker.CreateProducer(broker.ProducerOptions{Topic: agentID})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := tracehook.Publish(d, agentID, txn, func() error {
		_, err := p.SendProto(t.Context(), &pb.HTTPProxyRequest{Request: &pb.HTTPRequest{TransactionId: txn}, Timestamps: &pb.Timestamps{}})
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// answer is the agent's answer to txn on s (it closes the old stream only
// once it answered every request it received there).
func answer(t *testing.T, d *Dispatcher, s *HTTPStream, txn string) {
	t.Helper()
	if err := d.processHTTPInput(t.Context(), s, &pb.HTTPProxyResponse{Response: &pb.HTTPResponse{TransactionId: txn}, Timestamps: &pb.Timestamps{}}, time.Now()); err != nil {
		t.Fatal(err)
	}
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
	s.moved, s.moveDone = make(chan struct{}), make(chan struct{})
	d.Agents.Store("agent-1", s)
	serveTopic(t, d, s)
	mc.Data[directory.GetDispatcherCacheKey("agent-1")] = &directory.Value{PodIP: "10.0.0.1", Since: s.connectedAt.UnixNano()}
	return d, s, sink, mc, inv, drain
}

// A draining replica asks an agent that moves to connect again instead of
// letting it go; once the agent reports its new connection and its record
// names the new session, the replica stops routing to the old stream, then
// tells the agent no request follows (MOVE_DONE), and the session ends when
// the agent closes the stream. A request the ingress picked this replica
// for before routing stopped (its route lease) holds MOVE_DONE back until
// its first message is on the agent's topic, and goes out before it
// (spec/README.md finding 6: a timer sent MOVE_DONE past it).
func TestAgentMovesBeforeTheReplicaLetsItGo(t *testing.T) {
	d, s, sink, mc, inv, drain := movingSession(t)
	release, ok := d.RouteHere("agent-1") // picked: its body's first chunk still being read
	if !ok {
		t.Fatal("no route lease on a session that routes")
	}
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
	if _, ok := d.RouteHere("agent-1"); ok {
		t.Fatal("a route lease on a session that no longer routes")
	}
	time.Sleep(time.Second) // the old settle timer was 500 ms
	if steps := sink.steps(); len(steps) != 1 {
		t.Fatalf("MOVE_DONE while a request routed here was not yet published: %v", steps)
	}
	publishTo(t, d, "agent-1", "txn-picked")
	release()
	waitMove(t, "MOVE_DONE", func() bool { return len(sink.steps()) == 2 && sink.steps()[1] == pb.Move_MOVE_DONE })
	if sink.mu.Lock(); sink.before[1] != 1 {
		t.Fatalf("MOVE_DONE went out before the routed request (%d requests before it)", sink.before[1])
	}
	sink.mu.Unlock()
	select {
	case err := <-done:
		t.Fatalf("session ended (%v) before the agent closed the stream", err)
	case <-time.After(100 * time.Millisecond):
	}
	answer(t, d, s, "txn-picked")
	received <- nil // the agent answered and closed the stream
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

// A drain's move bound covers the move until this replica stops routing
// to the old stream: a request it routed here before that, whose first
// message takes longer than the bound (a slow upload's first chunk), is
// not failed by it; MOVE_DONE follows it and the agent closes the stream
// (spec/README.md finding 21, Move.tla DrainBound).
func TestDrainBoundEndsOnceRoutingStopped(t *testing.T) {
	d, s, sink, mc, inv, drain := movingSession(t)
	d.moveBound = 200 * time.Millisecond
	release, ok := d.RouteHere("agent-1")
	if !ok {
		t.Fatal("no route lease on a session that routes")
	}
	received := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- d.WatchAgentSession(t.Context(), "agent-1", s, inv, received) }()
	drain()
	waitMove(t, "MOVE_RECONNECT", func() bool { return len(sink.steps()) == 1 })
	mc.Mu.Lock()
	mc.Data[directory.GetDispatcherCacheKey("agent-1")] = &directory.Value{PodIP: "10.0.0.2", Since: time.Now().UnixNano()}
	mc.Mu.Unlock()
	d.processHTTPInput(t.Context(), s, &pb.HTTPProxyResponse{Move: pb.Move_MOVE_CONNECTED}, time.Now()) //nolint:errcheck
	waitMove(t, "routing stopped", func() bool { return !d.IsAgentConnected("agent-1") })
	select {
	case err := <-done:
		t.Fatalf("the drain's move bound ended the session (%v) while a request routed here was held", err)
	case <-time.After(3 * d.moveBound):
	}
	publishTo(t, d, "agent-1", "txn-slow")
	release()
	waitMove(t, "MOVE_DONE", func() bool { return len(sink.steps()) == 2 && sink.steps()[1] == pb.Move_MOVE_DONE })
	answer(t, d, s, "txn-slow")
	received <- nil // the agent answered and closed the stream
	if err := <-done; err != nil {
		t.Fatalf("session ended with %v", err)
	}
}
