// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/directory"
	"github.com/spf13/viper"
)

// A model-based test of relay-server's spec/Replicas.tla on this module's
// code: each walk (spec/testdata/replicas-walks.ndjson, made by
// relay-server's scripts/spec.sh walks from spec/ReplicasWalk.tla) is
// replayed on two replicas, each a TCPDispatcher with its streamer
// inventory over one store. A step runs the code its action names
// (claimAgentRecord, renewAgentRecord, releaseAgentRecord, enterRotation,
// yield, unregister, agentStream, attach, GetIP), each store write of the
// record its own step as in the model (the store holds a claim's write
// until the walk's ClaimSet releases it; a renewal and a delete check and
// write in one step, Cache.SetAddIf and Cache.DelIf, the model's Atomic
// "renew" and "delete", so they are not held). After every step the
// record, the Seq counter, each connection's Seq, phase and pending write,
// each replica's routing and each session's outcome must be the model's.
//
// Not driven: HandleConn's and ServeLinkTCP's I/O, streamFor's wait and the
// replica link (the replay takes their decisions from agentStream and
// GetIP, as they do); sendLoop's timers (a renewal is the walk's step, its
// ErrSuperseded answered by yield as sendLoop does); ConnectTCP's deferred
// release check (mirrored in End).

const rwAgent = "agent-1"

type rwState struct {
	A   string `json:"a"`
	Ctx struct {
		C int    `json:"c"`
		R string `json:"r"`
		K int    `json:"k"`
	} `json:"ctx"`
	Ctr int64 `json:"ctr"`
	Rec struct {
		Pod   string `json:"pod"`
		Seq   int64  `json:"seq"`
		Since int    `json:"since"`
	} `json:"rec"`
	Conn []struct {
		Seq   int64  `json:"seq"`
		Ph    string `json:"ph"`
		R     string `json:"r"`
		Rpend bool   `json:"rpend"`
		Dpend string `json:"dpend"`
	} `json:"conn"`
	Sess []struct {
		C   int    `json:"c"`
		St  string `json:"st"`
		To  string `json:"to"`
		Why string `json:"why"`
	} `json:"sess"`
	Up map[string]bool `json:"up"`
}

func readReplicasWalks(t *testing.T) [][]rwState {
	t.Helper()
	f, err := os.Open("../../spec/testdata/replicas-walks.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close() //nolint:errcheck
	var walks [][]rwState
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var s rwState
		if err := json.NewDecoder(bytes.NewReader(sc.Bytes())).Decode(&s); err != nil {
			t.Fatalf("walk line: %v", err)
		}
		if s.A == "Init" {
			walks = append(walks, nil)
		}
		walks[len(walks)-1] = append(walks[len(walks)-1], s)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return walks
}

func rwPod(r string) string { return "10.0.0." + strings.TrimPrefix(r, "r") }

var (
	rwRecKey = directory.GetStreamerCacheKey(rwAgent)
	rwCtrKey = "nfltr:agent-session-seq:" + rwRecKey // directory.seqKey
)

var errRWCrashed = errors.New("the replica crashed")

// rwHeld is a store write held at the store until its step.
type rwHeld struct {
	seq int64 // a SET's Seq
	go_ chan bool
}

// rwStore holds each SET and DEL of the record, and a claim's SetAddIf,
// until released; a renewal's SetAddIf (renewing set) is one step, as the
// model's Atomic "renew".
type rwStore struct {
	directory.Cache
	held     chan *rwHeld
	renewing atomic.Bool
}

func (s *rwStore) hold(seq int64) error {
	h := &rwHeld{seq: seq, go_: make(chan bool)}
	s.held <- h
	if !<-h.go_ {
		return errRWCrashed
	}
	return nil
}

func (s *rwStore) Set(ctx context.Context, key string, v any, exp time.Duration) (string, error) {
	if key == rwRecKey {
		var seq int64
		if val, ok := v.(directory.Value); ok {
			seq = val.Seq
		}
		if err := s.hold(seq); err != nil {
			return "", err
		}
	}
	return s.Cache.Set(ctx, key, v, exp)
}

func (s *rwStore) SetAddIf(ctx context.Context, key, ctr string, match func(*directory.Value, int64) bool, v directory.Value, exp time.Duration, member string, sets ...string) (bool, error) {
	if key == rwRecKey && !s.renewing.Load() {
		if err := s.hold(v.Seq); err != nil {
			return false, err
		}
	}
	return s.Cache.SetAddIf(ctx, key, ctr, match, v, exp, member, sets...)
}

func (s *rwStore) Del(ctx context.Context, keys ...string) (int64, error) {
	if slices.Contains(keys, rwRecKey) {
		if err := s.hold(0); err != nil {
			return 0, err
		}
	}
	return s.Cache.Del(ctx, keys...)
}

type rwConn struct {
	r        string
	since    time.Time
	s        directory.Session
	as       *tcpAgentStream
	claiming bool
	ended    bool
	held     *rwHeld
	done     chan error
	dpend    string
}

type rwSess struct {
	st, to, why string
	at          string
	c           int
	id          string
	cut         bool
}

type rwReplay struct {
	t     *testing.T
	ctx   context.Context
	t0    time.Time
	mc    *directory.MemoryCache
	store *rwStore
	inv   map[string]directory.IPInventory
	d     map[string]*TCPDispatcher // nil: crashed
	conn  []*rwConn
	sess  []*rwSess
}

func newRWReplay(t *testing.T, nConn, nSess int) *rwReplay {
	t0 := time.Date(2026, 11, 4, 1, 0, 0, 0, time.UTC)
	mc := directory.NewMemoryCacheClock(func() time.Time { return t0 })
	p := &rwReplay{t: t, ctx: context.Background(), t0: t0, mc: mc,
		store: &rwStore{Cache: mc, held: make(chan *rwHeld)},
		inv:   map[string]directory.IPInventory{}, d: map[string]*TCPDispatcher{}}
	for _, r := range []string{"r1", "r2"} {
		inv, err := directory.NewIPInventory(slog.New(slog.DiscardHandler), p.store, time.Hour, "127.0.0.1", directory.GetStreamerCacheKey)
		if err != nil {
			t.Fatal(err)
		}
		p.inv[r] = inv
		p.d[r] = NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, inv)
	}
	for c := 1; c <= nConn; c++ {
		p.conn = append(p.conn, &rwConn{since: t0.Add(time.Duration(c) * time.Second), dpend: "no"})
	}
	for range nSess {
		p.sess = append(p.sess, &rwSess{st: "none", to: "none"})
	}
	return p
}

// run starts f as connection c's call on its replica and returns once it
// returned or reached a held write.
func (p *rwReplay) run(c *rwConn, f func() error) error {
	p.t.Setenv("POD_IP", rwPod(c.r))
	c.done = make(chan error, 1)
	go func() { c.done <- f() }()
	return p.wait(c)
}

func (p *rwReplay) wait(c *rwConn) error {
	select {
	case h := <-p.store.held:
		c.held = h
		return nil
	case err := <-c.done:
		c.done = nil
		return err
	}
}

// release lets c's held write go (ok) or fail (its replica crashed).
func (p *rwReplay) release(c *rwConn, ok bool) error {
	if c.held == nil {
		return fmt.Errorf("no write held")
	}
	p.t.Setenv("POD_IP", rwPod(c.r))
	h := c.held
	c.held = nil
	h.go_ <- ok
	return p.wait(c)
}

func (p *rwReplay) step(m rwState) error {
	ctx, ttl := p.ctx, time.Hour
	var c *rwConn
	if m.Ctx.C > 0 {
		c = p.conn[m.Ctx.C-1]
	}
	var k *rwSess
	if m.Ctx.K > 0 {
		k = p.sess[m.Ctx.K-1]
	}
	switch m.A {
	case "Init", "Die": // Die: the agent's side; nothing at the relay
	case "Connect":
		c.r = m.Ctx.R
		c.claiming = true
	case "ClaimIncr":
		return p.run(c, func() error {
			s, err := claimAgentRecord(ctx, p.inv[c.r], rwAgent, directory.Session{Since: c.since}, nil, nil, ttl)
			c.s = s
			return err
		})
	case "ClaimSet":
		if err := p.release(c, true); err != nil {
			return err
		}
		c.claiming = false
	case "Register":
		c.as = NewTCPAgentStream()
		c.as.agent, c.as.session = rwAgent, c.s
		p.d[c.r].enterRotation(rwAgent, c.as)
	case "RenewGet":
		p.store.renewing.Store(true)
		err := p.run(c, func() error {
			return renewAgentRecord(ctx, p.inv[c.r], rwAgent, c.s, nil, nil, ttl)
		})
		p.store.renewing.Store(false)
		if errors.Is(err, directory.ErrSuperseded) {
			p.d[c.r].yield(rwAgent, c.as) // sendLoop's answer
			return nil
		}
		return err
	case "End":
		d := p.d[c.r]
		if c.as != nil {
			d.unregister(rwAgent, c.as)
		}
		c.ended = true
		// ConnectTCP's deferred release: only when no stream of the agent
		// is left here.
		if d.agentStream(rwAgent) == nil {
			c.dpend = "get"
		}
	case "DelGet":
		err := p.run(c, func() error { return releaseAgentRecord(ctx, p.inv[c.r], rwAgent, c.s) })
		c.dpend = "no"
		if c.held != nil {
			c.dpend = "del"
		}
		return err
	case "Expire":
		_, err := p.mc.Del(ctx, rwRecKey)
		return err
	case "Crash":
		r := m.Ctx.R
		for _, x := range p.conn {
			if x.r != r || x.ended || (!x.claiming && x.as == nil && x.s.Seq == 0) {
				continue
			}
			if x.held != nil {
				if err := p.release(x, false); !errors.Is(err, errRWCrashed) {
					return fmt.Errorf("a held write after its replica crashed: %v", err)
				}
			}
			if x.as != nil {
				p.d[r].unregister(rwAgent, x.as) // its streams end: sessions cut
			}
			x.claiming, x.ended, x.dpend = false, true, "no"
		}
		p.d[r] = nil
		for _, s := range p.sess {
			switch {
			case (s.st == "arrived" || s.st == "fwd") && s.at == r:
				s.st = "cut"
			case s.st == "fwd" && s.to == r:
				s.st, s.why = "refused", "unreachable"
			}
		}
	case "Restart":
		p.d[m.Ctx.R] = NewTCPDispatcher(slog.New(slog.DiscardHandler), nil, p.inv[m.Ctx.R])
	case "Arrive":
		k.st, k.at = "arrived", m.Ctx.R
	case "Route":
		d := p.d[k.at]
		if as := d.agentStream(rwAgent); as != nil {
			return p.deliver(k, d, as)
		}
		p.t.Setenv("POD_IP", rwPod(k.at))
		ip, err := p.inv[k.at].GetIP(ctx, rwAgent)
		switch {
		case errors.Is(err, directory.ErrAgentNotConnected) || err == nil && ip == "127.0.0.1":
			k.st, k.why = "refused", "offline"
		case err != nil:
			return err
		default:
			to := "r" + strings.TrimPrefix(ip, "10.0.0.")
			if p.d[to] == nil {
				k.st, k.why = "refused", "unreachable"
			} else {
				k.st, k.to = "fwd", to
			}
		}
	case "Accept":
		d := p.d[k.to]
		if as := d.agentStream(rwAgent); as != nil {
			return p.deliver(k, d, as)
		}
		k.st, k.why = "refused", "offline"
	case "Finish":
		p.conn[k.c-1].as.removeSession(k.id)
		k.st = "done"
	default:
		return fmt.Errorf("unknown action %q", m.A)
	}
	return nil
}

func (p *rwReplay) deliver(k *rwSess, d *TCPDispatcher, as *tcpAgentStream) error {
	s := newTCPSession()
	s.abort = func() { k.cut = true }
	// The session's own index: two sessions of a walk on one stream must
	// not share an id (one would replace the other at attach).
	k.id = strconv.Itoa(slices.Index(p.sess, k)) + "-" + k.at
	got := d.attach(rwAgent, as, k.id, s)
	if got == nil {
		return fmt.Errorf("attach: no stream took the session")
	}
	for i, x := range p.conn {
		if x.as == got {
			k.st, k.c = "delivered", i+1
			return nil
		}
	}
	return fmt.Errorf("attach: a stream of no connection")
}

func (p *rwReplay) phase(c *rwConn) string {
	switch {
	case c.r == "":
		return "idle"
	case c.ended:
		return "ended"
	case c.claiming:
		return "claiming"
	case c.as == nil:
		return "set"
	}
	d := p.d[c.r]
	d.Mu.RLock()
	in := slices.Contains(d.All[rwAgent], c.as)
	d.Mu.RUnlock()
	if in {
		return "live"
	}
	return "yielded"
}

func (p *rwReplay) compare(m rwState) error {
	v, err := p.mc.Get(p.ctx, rwRecKey)
	if err != nil && !errors.Is(err, directory.ErrNotFound) {
		return err
	}
	if m.Rec.Pod == "none" {
		if v != nil {
			return fmt.Errorf("record %+v, model none", *v)
		}
	} else if v == nil || v.PodIP != rwPod(m.Rec.Pod) || v.Seq != m.Rec.Seq || v.Since != p.t0.Add(time.Duration(m.Rec.Since)*time.Second).UnixNano() {
		return fmt.Errorf("record %+v, model %+v", v, m.Rec)
	}
	var n int64
	if cv, err := p.mc.Get(p.ctx, rwCtrKey); err == nil && cv != nil {
		if n, err = strconv.ParseInt(cv.PodIP, 10, 64); err != nil {
			return err
		}
	}
	if n != m.Ctr {
		return fmt.Errorf("counter %d, model %d", n, m.Ctr)
	}
	for i, c := range p.conn {
		mc := m.Conn[i]
		seq := c.s.Seq
		if c.claiming && c.held != nil {
			seq = c.held.seq
		}
		ph := p.phase(c)
		if ph != mc.Ph || (ph != "idle" && ph != "claiming" || c.held != nil) && seq != mc.Seq {
			return fmt.Errorf("connection %d: %s Seq %d, model %s Seq %d", i+1, ph, seq, mc.Ph, mc.Seq)
		}
		if rp := c.held != nil && !c.claiming && c.dpend != "del"; rp != mc.Rpend || c.dpend != mc.Dpend {
			return fmt.Errorf("connection %d: renew held %v, delete %s; model %v, %s", i+1, rp, c.dpend, mc.Rpend, mc.Dpend)
		}
	}
	for _, r := range []string{"r1", "r2"} {
		if m.Up[r] != (p.d[r] != nil) {
			return fmt.Errorf("replica %s up %v, model %v", r, p.d[r] != nil, m.Up[r])
		}
		if p.d[r] == nil {
			continue
		}
		want := 0 // Replicas.tla Routing(r): the live connection there of the newest session (Seq, then index)
		for i := range m.Conn {
			if m.Conn[i].R == r && m.Conn[i].Ph == "live" && (want == 0 || m.Conn[i].Seq >= m.Conn[want-1].Seq) {
				want = i + 1
			}
		}
		got := 0
		if as := p.d[r].agentStream(rwAgent); as != nil {
			for i, c := range p.conn {
				if c.as == as {
					got = i + 1
				}
			}
		}
		if got != want {
			return fmt.Errorf("replica %s routes to connection %d, model %d", r, got, want)
		}
	}
	for i, k := range p.sess {
		st := k.st
		if st == "delivered" && k.cut {
			st = "cut"
		}
		ms := m.Sess[i]
		if st != ms.St || st == "refused" && k.why != ms.Why || st == "fwd" && k.to != ms.To || (st == "delivered" || st == "cut" && k.c > 0) && k.c != ms.C {
			return fmt.Errorf("session %d: %s %s->%s c%d, model %s %s->%s c%d", i+1, st, k.why, k.to, k.c, ms.St, ms.Why, ms.To, ms.C)
		}
	}
	return nil
}

func (p *rwReplay) close() {
	for _, c := range p.conn {
		if c.held != nil {
			_ = p.release(c, false)
		}
	}
}

func replayReplicasWalk(t *testing.T, walk []rwState) (err error) {
	p := newRWReplay(t, len(walk[0].Conn), len(walk[0].Sess))
	defer p.close()
	for n, m := range walk {
		if err := p.step(m); err != nil {
			return fmt.Errorf("step %d %s %+v: %w", n, m.A, m.Ctx, err)
		}
		if err := p.compare(m); err != nil {
			return fmt.Errorf("step %d %s %+v: %w", n, m.A, m.Ctx, err)
		}
	}
	return nil
}

func TestReplicasModelWalks(t *testing.T) {
	viper.Set("pod_ip.env_var", "POD_IP")
	t.Cleanup(func() { viper.Set("pod_ip.env_var", "") })
	walks := readReplicasWalks(t)
	seen := map[string]int{}
	for i, w := range walks {
		if err := replayReplicasWalk(t, w); err != nil {
			t.Fatalf("walk %d: %v", i+1, err)
		}
		for _, s := range w {
			seen[s.A]++
			for _, c := range s.Conn {
				seen["ph:"+c.Ph]++
			}
			for _, k := range s.Sess {
				seen["st:"+k.St]++
			}
		}
	}
	// Every relay step, a yield, a forward, a refusal and a cut session
	// (not Finish: the client's end, rare in random walks since a Die
	// disables it; removeSession is all it runs here).
	for _, a := range []string{"Connect", "ClaimIncr", "ClaimSet", "Register", "RenewGet", "Die", "End",
		"DelGet", "Expire", "Crash", "Restart", "Arrive", "Route", "Accept",
		"ph:yielded", "st:fwd", "st:refused", "st:cut"} {
		if seen[a] == 0 {
			t.Errorf("no walk takes %s: regenerate spec/testdata/replicas-walks.ndjson (relay-server scripts/spec.sh walks)", a)
		}
	}
}
