// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"context"
	"fmt"
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/agentwire"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/nfltr/relay-core/core/directory"
)

// moverConn is one ConnectHTTP stream of a fake agent that moves (as the
// relay client's agent does): what the relay sends goes to steps, what the
// agent sends comes from recv (closed: the agent closed the stream).
type moverConn struct {
	ctx        context.Context
	accepted   chan struct{}
	acceptOnce sync.Once
	steps      chan pb.Move
	recv       chan *pb.HTTPProxyResponse
	hungUp     chan struct{}
	hangUpOnce sync.Once
	ended      chan error // ConnectHTTP's return
}

// hangUp closes the stream (the agent's side).
func (c *moverConn) hangUp() { c.hangUpOnce.Do(func() { close(c.hungUp) }) }

func (c *moverConn) Send(r *pb.HTTPProxyRequest) error {
	if m := r.GetMove(); m != pb.Move_MOVE_UNSPECIFIED {
		c.steps <- m
	}
	return nil
}

func (c *moverConn) Recv() (*pb.HTTPProxyResponse, error) {
	select {
	case m := <-c.recv:
		return m, nil
	case <-c.hungUp:
		return nil, io.EOF
	}
}

func (c *moverConn) Context() context.Context { return c.ctx }
func (c *moverConn) SendHeader(metadata.MD) error {
	c.acceptOnce.Do(func() { close(c.accepted) })
	return nil
}
func (c *moverConn) SetHeader(metadata.MD) error { return nil }
func (c *moverConn) SetTrailer(metadata.MD)      {}
func (c *moverConn) SendMsg(any) error           { return nil }
func (c *moverConn) RecvMsg(any) error           { return nil }

// moverFleet is replicas sharing one directory and fake agents connecting
// through lb, as a Service spreads new connections over its replicas.
type moverFleet struct {
	t        *testing.T
	replicas []*Dispatcher
	lb       atomic.Pointer[[]*Dispatcher]
	next     atomic.Int64
	refused  atomic.Int64 // connections a replica refused
	broken   atomic.Int64 // sessions the relay ended, not the agent
	moves    atomic.Int64 // MOVE_RECONNECTs the agents received
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    map[*moverConn]bool
}

func newMoverFleet(t *testing.T, n int) *moverFleet {
	viper.Set("pod_ip.env_var", "TEST_POD_IP")
	t.Cleanup(viper.Reset)
	t.Setenv("TEST_POD_IP", "10.0.0.1")
	inv, _ := directory.NewIPInventory(fake.TestLogger(), directory.NewMemoryCache(), time.Second, "127.0.0.1", directory.GetDispatcherCacheKey)
	f := &moverFleet{t: t, conns: map[*moverConn]bool{}}
	for range n {
		d, cancel := newTestDispatcher(t)
		t.Cleanup(cancel)
		d.IPInventory, d.PodDialer = inv, staticRefusingDialer{}
		d.rebalanceMinAge = 0 // these tests' sessions are all young
		f.replicas = append(f.replicas, d)
	}
	t.Cleanup(f.close)
	return f
}

// route sends new connections to these replicas, in turn.
func (f *moverFleet) route(rs ...*Dispatcher) { f.lb.Store(&rs) }

// connect opens a session of agent id, tried again while refused.
func (f *moverFleet) connect(id string) *moverConn {
	for {
		rs := *f.lb.Load()
		d := rs[int(f.next.Add(1))%len(rs)]
		md := metadata.Pairs("x-apikey-agentID", id, "x-auth-method", "apikey", agentwire.MoveKey, "1")
		c := &moverConn{ctx: testPrincipal(metadata.NewIncomingContext(context.Background(), md)), accepted: make(chan struct{}),
			steps: make(chan pb.Move, 4), recv: make(chan *pb.HTTPProxyResponse, 4), hungUp: make(chan struct{}), ended: make(chan error, 1)}
		go func() { c.ended <- d.ConnectHTTP(c) }()
		select {
		case <-c.accepted:
			f.mu.Lock()
			f.conns[c] = true
			f.mu.Unlock()
			f.wg.Add(1)
			go f.serve(id, c)
			return c
		case err := <-c.ended:
			if status.Code(err) != codes.Unavailable {
				f.t.Errorf("agent %s refused with %v", id, err)
			}
			f.refused.Add(1)
			time.Sleep(time.Millisecond)
		}
	}
}

// serve is the agent's side of one connection: on MOVE_RECONNECT it
// connects again, then reports MOVE_CONNECTED; on MOVE_DONE it closes the
// stream.
func (f *moverFleet) serve(id string, c *moverConn) {
	defer f.wg.Done()
	for {
		select {
		case step := <-c.steps:
			switch step {
			case pb.Move_MOVE_RECONNECT:
				f.moves.Add(1)
				f.connect(id)
				c.recv <- &pb.HTTPProxyResponse{Move: pb.Move_MOVE_CONNECTED}
			case pb.Move_MOVE_DONE:
				f.mu.Lock()
				delete(f.conns, c)
				f.mu.Unlock()
				c.hangUp()
				<-c.ended
				return
			}
		case err := <-c.ended:
			f.mu.Lock()
			agentClosed := !f.conns[c]
			delete(f.conns, c)
			f.mu.Unlock()
			if !agentClosed {
				f.broken.Add(1)
				f.t.Errorf("the relay ended a session of %s: %v", id, err)
			}
			return
		}
	}
}

func (f *moverFleet) close() {
	f.mu.Lock()
	for c := range f.conns {
		delete(f.conns, c)
		c.hangUp()
	}
	f.mu.Unlock()
	f.wg.Wait()
}

// settled reports every agent with exactly one session, routed to, and
// none moving.
func (f *moverFleet) settled(n int) bool {
	routed := 0
	for _, d := range f.replicas {
		routed += d.AgentsHere()
		moving := false
		d.Agents.Range(func(_ string, s *HTTPStream) bool {
			moving = s.moving.Load()
			return !moving
		})
		if moving {
			return false
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return routed == n && len(f.conns) == n
}

func (f *moverFleet) tick() int {
	total, asked := 0, 0
	for _, d := range f.replicas {
		total += d.AgentsHere()
	}
	for _, d := range f.replicas {
		asked += d.rebalanceAgents(total, len(f.replicas))
	}
	return asked
}

// TestRebalanceAfterScaleUp: the replicas holding every agent are joined
// by one that holds none (2 -> 3 as well: two replicas shed at once, each
// refusing the other's moving agents); paced moves bring every replica
// within one of the mean, each agent moving make before break (the relay
// never ends a session; every agent is routed to on some replica
// throughout), and once balanced nothing moves.
func TestRebalanceAfterScaleUp(t *testing.T) {
	for _, tc := range []struct{ n, before, after int }{{31, 1, 2}, {62, 2, 3}} {
		t.Run(fmt.Sprintf("%d agents, %d to %d replicas", tc.n, tc.before, tc.after), func(t *testing.T) {
			n := tc.n
			f := newMoverFleet(t, tc.after)
			f.route(f.replicas[:tc.before]...)
			for i := range n {
				f.connect(fmt.Sprint("agent-", i))
			}
			f.route(f.replicas...) // the new replica joins

			var gaps atomic.Int64 // an agent routed to on no replica: a failed visit
			stop := make(chan struct{})
			visited := make(chan struct{})
			go func() {
				defer close(visited)
				for {
					select {
					case <-stop:
						return
					default:
					}
					for i := range n {
						id, on := fmt.Sprint("agent-", i), false
						for _, d := range f.replicas {
							_, here := d.Agents.Load(id)
							on = on || here
						}
						if !on {
							gaps.Add(1)
						}
					}
					time.Sleep(time.Millisecond)
				}
			}()

			spread := func() (s []int) {
				for _, d := range f.replicas {
					s = append(s, d.AgentsHere())
				}
				return s
			}
			for tick := 0; ; tick++ {
				if tick == 10 {
					t.Fatalf("not balanced after %d ticks: %v", tick, spread())
				}
				if f.tick() == 0 {
					break
				}
				waitMove(t, "the tick's moves", func() bool { return f.settled(n) })
			}
			close(stop)
			<-visited
			mean := float64(n) / float64(tc.after)
			for i, d := range f.replicas {
				if got := float64(d.AgentsHere()); got < mean-1 || got > mean+1 {
					t.Errorf("replica %d holds %v agents, want %.1f ± 1", i, got, mean)
				}
				if d.overShare.Load() {
					t.Errorf("replica %d, balanced, still refuses new agents", i)
				}
			}
			if gaps.Load() > 0 || f.broken.Load() > 0 {
				t.Fatalf("%d visits found no replica routing to the agent; %d sessions ended by the relay", gaps.Load(), f.broken.Load())
			}
			if f.refused.Load() == 0 {
				t.Fatal("no connection refused: a replica above its share must refuse reconnects")
			}
			t.Logf("balanced: %v after %d moves (%d connections refused)", spread(), f.moves.Load(), f.refused.Load())
		})
	}
}

// TestRebalancePaced: a tick asks at most rebalanceMovesPerTick agents to
// move; a replica alone, or without peers, moves none.
func TestRebalancePaced(t *testing.T) {
	const n = 40
	f := newMoverFleet(t, 2)
	a, b := f.replicas[0], f.replicas[1]
	f.route(a)
	for i := range n {
		f.connect(fmt.Sprint("agent-", i))
	}
	if got := a.rebalanceAgents(n, 1); got != 0 {
		t.Fatalf("alone: asked %d agents to move", got)
	}
	single, cancel := newTestDispatcher(t)
	t.Cleanup(cancel)
	single.Agents.Store("agent-0", &HTTPStream{ID: "agent-0", rebalance: make(chan struct{})})
	if got := single.rebalanceAgents(n, 2); got != 0 {
		t.Fatalf("without peers: asked %d agents to move", got)
	}

	f.route(b)
	if got := a.rebalanceAgents(n, 2); got != a.rebalanceMovesPerTick {
		t.Fatalf("one tick asked %d of the 20 excess agents to move, want %d", got, a.rebalanceMovesPerTick)
	}
	waitMove(t, "the tick's moves", func() bool { return f.settled(n) })
	if f.moves.Load() != int64(a.rebalanceMovesPerTick) || b.AgentsHere() != a.rebalanceMovesPerTick {
		t.Fatalf("%d moves, B holds %d; want %d", f.moves.Load(), b.AgentsHere(), a.rebalanceMovesPerTick)
	}
}

// TestReplicaOverShareRefusesMovedAgents: above its share a replica
// refuses an agent whose record another replica holds (a move or a
// reconnect: clients try those again, on another replica), and admits a new
// agent, whose first connection a client does not retry (kind soak
// 2026-10-21: a refused first connection ended the agent); at its share it
// refuses neither.
func TestReplicaOverShareRefusesMovedAgents(t *testing.T) {
	f := newMoverFleet(t, 2)
	a, b := f.replicas[0], f.replicas[1]
	f.route(a)
	for i := range 4 {
		f.connect(fmt.Sprint("agent-", i))
	}
	f.route(b)
	f.connect("agent-held")
	a.rebalanceMovesPerTick = 0 // above its share, asking none to move yet
	a.rebalanceAgents(5, 2)
	if err := a.ConnectHTTP(&mockConnectHTTPServer{ctx: makeGRPCContext("agent-held")}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a replica above its share admitted an agent held by another: %v", err)
	}
	if a.refusesAgents(context.Background(), "agent-new") {
		t.Fatal("a replica above its share refused a new agent's first connection")
	}
	a.rebalanceAgents(10, 2)
	if a.refusesAgents(context.Background(), "agent-held") {
		t.Fatal("a replica at its share refuses moved agents")
	}
}

// TestRebalanceMoveGivenUpKeepsTheAgent: an agent that does not report a
// new connection within moveBound stays (the relay does not let it go),
// renews its record again, and is not asked again.
func TestRebalanceMoveGivenUpKeepsTheAgent(t *testing.T) {
	d, s, sink, _, inv, _ := movingSession(t)
	d.moveBound = 100 * time.Millisecond
	s.rebalance = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- d.WatchAgentSession(t.Context(), "agent-1", s, inv, make(chan error)) }()
	if got := d.rebalanceAgents(0, 2); got != 1 {
		t.Fatalf("asked %d, want 1", got)
	}
	waitMove(t, "MOVE_RECONNECT", func() bool { return len(sink.steps()) == 1 })
	waitMove(t, "the move given up", func() bool { return !s.moving.Load() })
	select {
	case err := <-done:
		t.Fatalf("the relay let the agent go: %v", err)
	case <-time.After(3 * d.moveBound):
	}
	if !d.IsAgentConnected("agent-1") || d.rebalanceAgents(0, 2) != 0 {
		t.Fatal("the agent is no longer routed to, or was asked again")
	}
}

// TestRebalanceLeavesYoungSessions: an agent that just arrived (by a
// drain's or another replica's move) is not asked to move on before the
// fleet's counts show it (kind soak 2026-10-21, rollout: agents moved twice
// within seconds).
func TestRebalanceLeavesYoungSessions(t *testing.T) {
	f := newMoverFleet(t, 2)
	a := f.replicas[0]
	f.route(a)
	for i := range 4 {
		f.connect(fmt.Sprint("agent-", i))
	}
	f.route(f.replicas[1])
	a.rebalanceMinAge = time.Hour
	if asked := a.rebalanceAgents(4, 2); asked != 0 {
		t.Fatalf("asked %d agents that just connected to move", asked)
	}
	a.rebalanceMinAge = 0
	if asked := a.rebalanceAgents(4, 2); asked != 2 {
		t.Fatalf("asked %d agents to move, want 2", asked)
	}
	waitMove(t, "the moves", func() bool { return f.settled(4) })
}

// TestRebalanceLeavesAgentsCarryingTCPSessions (kind soak 2026-10-26: a
// held session cut ConnLost 30 s after its agent's rebalancing move, when
// the agent closed the connection it moved off): an agent whose TCP stream
// carries a session is not asked to move; once the session ends, it is.
func TestRebalanceLeavesAgentsCarryingTCPSessions(t *testing.T) {
	f := newMoverFleet(t, 2)
	a := f.replicas[0]
	f.route(a)
	f.connect("agent-0")
	f.connect("agent-1")
	f.route(f.replicas[1])
	a.rebalanceMinAge = 0
	a.TCPDispatcher = NewTCPDispatcher(a.Logger, nil, nil)
	as := &tcpAgentStream{Sessions: map[string]*tcpSession{"s1": {}}}
	a.TCPDispatcher.Streams = map[string]*tcpAgentStream{"agent-0": as, "agent-1": as}
	endSessions := func() { as.Mu.Lock(); clear(as.Sessions); as.Mu.Unlock() }
	defer endSessions() // a failed check lets the moves it asked for finish
	if asked := a.rebalanceAgents(2, 2); asked != 0 {
		t.Fatalf("asked %d agents carrying a TCP session to move", asked)
	}
	endSessions()
	if asked := a.rebalanceAgents(2, 2); asked != 1 {
		t.Fatalf("asked %d agents to move once their sessions ended, want 1", asked)
	}
	waitMove(t, "the move", func() bool { return f.settled(2) })
}

// tryConnect is the relay client's move connection (client/agent
// moveConnect) on d alone: tried every `every` for `window`; nil when every
// try was refused.
func (f *moverFleet) tryConnect(d *Dispatcher, id string, every, window time.Duration) *moverConn {
	until := time.Now().Add(window)
	for {
		md := metadata.Pairs("x-apikey-agentID", id, "x-auth-method", "apikey", agentwire.MoveKey, "1")
		c := &moverConn{ctx: testPrincipal(metadata.NewIncomingContext(context.Background(), md)), accepted: make(chan struct{}),
			steps: make(chan pb.Move, 4), recv: make(chan *pb.HTTPProxyResponse, 4), hungUp: make(chan struct{}), ended: make(chan error, 1)}
		go func() { c.ended <- d.ConnectHTTP(c) }()
		select {
		case <-c.accepted:
			return c
		case <-c.ended:
			f.refused.Add(1)
		}
		if time.Now().After(until) {
			return nil
		}
		time.Sleep(every)
	}
}

// TestRebalanceAgentReachingOneReplicaStays: agents that dial one
// replica's address (no load balancer: a two-replica compose stack
// naming one replica) land every try of a rebalancing move back on the
// replica moving them, refused. Once such a move is given up the agent
// stays: its next sessions are not asked again (each ask cost 40 refused
// connections, 5/s for the client's 8 s, on every session) and its
// reconnects are admitted although the replica is above its share.
//
// A try admitted after the move was given up is the agent landing back to
// stay, not a refusal missed: under load (Linux runner, -tags linkdebug)
// the step reached the agent late enough that its 60 ms of tries outlasted
// a 100 ms bound; a 300 ms bound only made
// that rarer. The 1 ms bound runs that case every time.
func TestRebalanceAgentReachingOneReplicaStays(t *testing.T) {
	for _, bound := range []time.Duration{300 * time.Millisecond, time.Millisecond} {
		t.Run(bound.String(), func(t *testing.T) { testRebalanceAgentReachingOneReplicaStays(t, bound) })
	}
}

func testRebalanceAgentReachingOneReplicaStays(t *testing.T, bound time.Duration) {
	f := newMoverFleet(t, 2)
	a := f.replicas[0]
	a.moveBound = bound
	const n = 4
	var mu sync.Mutex // conns: an agent that lands back swaps its session
	conns := map[string]*moverConn{}
	var trying atomic.Int32
	var serve func(id string, c *moverConn)
	serve = func(id string, c *moverConn) { // MOVE_RECONNECT: try a again, then stay
		for {
			select {
			case step := <-c.steps:
				if step == pb.Move_MOVE_RECONNECT {
					f.moves.Add(1)
					trying.Add(1)
					nc := f.tryConnect(a, id, 5*time.Millisecond, 60*time.Millisecond)
					if nc != nil { // it landed back: its new session replaces the old one
						if !a.staysHere(id) { // admitted only once given up (refusesAgents)
							t.Errorf("a replica moving %s admitted its new connection", id)
						}
						mu.Lock()
						conns[id] = nc
						mu.Unlock()
						c.hangUp()
						go serve(id, nc)
					}
					trying.Add(-1)
					if nc != nil {
						return
					}
				}
			case <-c.hungUp:
				return
			}
		}
	}
	session := func(id string) *moverConn {
		c := f.tryConnect(a, id, time.Millisecond, time.Second)
		if c == nil {
			t.Fatalf("%s's connection refused for a second", id)
		}
		go serve(id, c)
		return c
	}
	for i := range n {
		id := fmt.Sprint("agent-", i)
		conns[id] = session(id)
	}
	t.Cleanup(func() { // before the fleet's: no session outlives the test
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.hangUp()
			<-c.ended
		}
	})
	stayed := func() (k int) {
		a.stays.Range(func(any, any) bool { k++; return true })
		return k
	}
	moving := func() (k int) {
		a.Agents.Range(func(_ string, s *HTTPStream) bool {
			if s.moving.Load() {
				k++
			}
			return true
		})
		return k
	}
	ticks := func() { // each tick's moves given up before the next (waited for, not slept: a loaded machine gives up late)
		for range 3 {
			a.rebalanceAgents(n, 2)
			waitMove(t, "the tick's moves given up", func() bool { return moving() == 0 })
		}
	}
	ticks() // above its share (2 of 4), a asks every agent once, in turn
	waitMove(t, "every move given up", func() bool { return stayed() == n && trying.Load() == 0 })
	if f.moves.Load() != n {
		t.Fatalf("%d moves asked, want %d (each agent once)", f.moves.Load(), n)
	}
	refused := f.refused.Load()

	mu.Lock()
	for id, c := range conns { // every agent's next session
		if a.refusesAgents(context.Background(), id) {
			t.Errorf("above its share, a refuses the reconnect of %s, which reaches no other replica", id)
		}
		c.hangUp()
		<-c.ended
		conns[id] = session(id)
	}
	mu.Unlock()
	ticks()
	if got := f.moves.Load(); got != n {
		t.Fatalf("%d moves asked, want %d: agents whose move reached no other replica were asked again on their next session (%d more connections refused)", got, n, f.refused.Load()-refused)
	}
	if a.AgentsHere() != n || !a.overShare.Load() {
		t.Fatalf("a holds %d agents (want %d), over its share %v", a.AgentsHere(), n, a.overShare.Load())
	}
	if a.refusesAgents(context.Background(), "agent-held-elsewhere") {
		t.Fatal("refused an agent without a record")
	}
}
