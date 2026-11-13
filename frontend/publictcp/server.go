// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package publictcp is the relay's public TCP front: a port of the relay's
// public range leased to one agent's plain tcp service, reached by anyone
// at host:port with no client (a game server, an SSH guest). The relay
// never reads the bytes; it decides only who connects: the lease's IP
// allowlist, the endpoint's and each source's connection caps, the lease's
// hard end (accounts with no payment method) and its end by the journal
// (Cut). One audit line per visit, never the payload.
//
// The design and its checked spec: relay-server spec/PortLease.tla
// (Lookup*, Connect, Close, Apply, HardExpire) and nfltr
// docs/decisions/public-tcp-endpoints-2026-11-13.md.
package publictcp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nfltr/relay-core/core/frontend"
	"github.com/nfltr/relay-core/internal/platform/acceptloop"
	"github.com/nfltr/relay-core/platform/connlimit"
	"github.com/nfltr/relay-core/platform/metrics"
)

// ErrNoGrant is Lookup's answer for a port no lease holds.
var ErrNoGrant = errors.New("public tcp: no lease on this port")

// Grant is a lease as a visit needs it: whose service the port reaches and
// on what terms.
type Grant struct {
	ShareID  string
	AgentID  string
	Service  string // the agent's plain tcp service name
	Policy   frontend.SharePolicy
	HardEnd  time.Time // zero: none; else no visit lasts past it
	MaxConns int       // open visits at once; 0: no cap
}

// Config is a Server's wiring; Lookup and Route are required.
type Config struct {
	// Lookup reads the lease on port from the store (ErrNoGrant: none).
	Lookup func(port int) (Grant, error)
	// Route carries conn to the grant's agent service and returns when the
	// session ends; the Server closes conn after it.
	Route func(ctx context.Context, conn net.Conn, g Grant)
	Log   *slog.Logger
	// Source limits each visitor address (Listener is set here).
	Source      connlimit.Config
	IdleTimeout time.Duration // no bytes either way this long ends a visit; 0: none
	ReadTTL     time.Duration // a grant is cached this long; default 1 min
	MissTTL     time.Duration // a port with no lease is cached this long; default 5 s
}

// Server admits visits on the public ports.
type Server struct {
	cfg Config
	wg  sync.WaitGroup

	mu       sync.Mutex
	cache    map[int]cached
	visits   map[string]map[*visit]struct{} // by share id
	all      map[*visit]struct{}
	gen      uint64            // counts Cuts
	inflight int               // admissions between their read and their check
	cutAt    map[string]uint64 // share -> gen of its last Cut, kept while admissions are in flight
}

type cached struct {
	g     Grant
	miss  bool
	until time.Time
}

// New returns a Server for cfg.
func New(cfg Config) *Server {
	if cfg.ReadTTL <= 0 {
		cfg.ReadTTL = time.Minute
	}
	if cfg.MissTTL <= 0 {
		cfg.MissTTL = 5 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	cfg.Source.Listener = "public_tcp"
	return &Server{cfg: cfg, cache: map[int]cached{}, visits: map[string]map[*visit]struct{}{},
		all: map[*visit]struct{}{}, cutAt: map[string]uint64{}}
}

// Serve accepts on lns (Bind's) until ctx ends, then closes them and ends
// every visit.
func (s *Server) Serve(ctx context.Context, lns []net.Listener) error {
	var loops sync.WaitGroup
	for _, ln := range lns {
		port := ln.Addr().(*net.TCPAddr).Port
		ln = connlimit.Wrap(ln, s.cfg.Source)
		loops.Go(func() { s.accept(ctx, ln, port) })
	}
	<-ctx.Done()
	closeAll(lns)
	loops.Wait()
	s.mu.Lock()
	vs := make([]*visit, 0, len(s.all))
	for v := range s.all {
		vs = append(vs, v)
	}
	s.mu.Unlock()
	for _, v := range vs {
		v.end("shutdown")
	}
	return nil
}

// Wait returns once every visit has ended.
func (s *Server) Wait() { s.wg.Wait() }

func (s *Server) accept(ctx context.Context, ln net.Listener, port int) {
	var backoff acceptloop.Backoff
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() == nil && backoff.Retry(s.cfg.Log, "public_tcp", err) {
				continue
			}
			return
		}
		backoff.Reset()
		s.wg.Go(func() { s.serve(ctx, c, port) })
	}
}

func (s *Server) serve(ctx context.Context, c net.Conn, port int) {
	v, reason := s.admit(c, port)
	if v == nil {
		s.cfg.Log.Debug("public tcp: visit refused", "port", port, "remote", c.RemoteAddr(), "reason", reason)
		metrics.IncrConnRefusals("public_tcp", reason)
		_ = c.Close()
		return
	}
	start := time.Now()
	s.cfg.Route(ctx, v, v.g)
	v.end("closed")
	s.forget(v)
	s.cfg.Log.Info("public tcp: visit ended", "share", v.g.ShareID, "port", port, "remote", c.RemoteAddr(),
		"agent_id", v.g.AgentID, "service", v.g.Service, "bytes_in", v.in.Load(), "bytes_out", v.out.Load(),
		"duration", time.Since(start).Round(time.Millisecond), "end", *v.why.Load())
}

// admit reads the port's grant (cache or store) and registers the visit
// under one lock with its checks (spec Connect: check-then-register is a
// mutant). A Cut of the grant's share after the read (cutAt, kept while an
// admission is in flight) refuses the visit and keeps the read out of the
// cache (spec LookupStore; mutant lookup-race).
func (s *Server) admit(c net.Conn, port int) (*visit, string) {
	s.mu.Lock()
	s.inflight++
	since := s.gen
	e, hit := s.cache[port]
	hit = hit && time.Now().Before(e.until)
	s.mu.Unlock()

	var g Grant
	var err error
	switch {
	case !hit:
		g, err = s.cfg.Lookup(port)
	case e.miss:
		err = ErrNoGrant
	default:
		g = e.g
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		if s.inflight--; s.inflight == 0 {
			clear(s.cutAt)
		}
	}()
	raced := err == nil && s.cutAt[g.ShareID] > since
	now := time.Now()
	if !hit {
		switch {
		case errors.Is(err, ErrNoGrant):
			// ponytail: a lease issued while this read was in flight is
			// refused for up to MissTTL; key misses by gen if that matters.
			s.cache[port] = cached{miss: true, until: now.Add(s.cfg.MissTTL)}
		case err == nil && !raced:
			s.cache[port] = cached{g: g, until: now.Add(s.cfg.ReadTTL)}
		}
	}
	switch {
	case errors.Is(err, ErrNoGrant):
		return nil, "no_lease"
	case err != nil:
		s.cfg.Log.Warn("public tcp: lease lookup failed", "port", port, "error", err)
		return nil, "lookup_failed"
	case raced:
		return nil, "ended"
	case !g.Policy.Admits(sourceIP(c)):
		return nil, "not_allowed"
	case !g.HardEnd.IsZero() && !now.Before(g.HardEnd):
		return nil, "hard_end"
	case g.MaxConns > 0 && len(s.visits[g.ShareID]) >= g.MaxConns:
		return nil, "endpoint_cap"
	}
	v := &visit{Conn: c, g: g, idle: s.cfg.IdleTimeout}
	v.last.Store(now.UnixNano())
	if s.visits[g.ShareID] == nil {
		s.visits[g.ShareID] = map[*visit]struct{}{}
	}
	s.visits[g.ShareID][v] = struct{}{}
	s.all[v] = struct{}{}
	v.tm.Lock()
	defer v.tm.Unlock()
	if !g.HardEnd.IsZero() {
		v.hard = time.AfterFunc(g.HardEnd.Sub(now), func() { v.end("hard_end") })
	}
	if v.idle > 0 {
		v.idler = time.AfterFunc(v.idle, v.checkIdle)
	}
	return v, ""
}

// Cut ends a lease on this relay (the journal's revoke, a release, a
// policy change): its cached grant is forgotten, its held visits end, and an
// admission in flight on it is refused. It returns the visits it ended.
func (s *Server) Cut(shareID string) int {
	s.mu.Lock()
	s.gen++
	if s.inflight > 0 {
		s.cutAt[shareID] = s.gen
	}
	for port, e := range s.cache {
		if !e.miss && e.g.ShareID == shareID {
			delete(s.cache, port)
		}
	}
	vs := make([]*visit, 0, len(s.visits[shareID]))
	for v := range s.visits[shareID] {
		vs = append(vs, v)
	}
	s.mu.Unlock()
	for _, v := range vs {
		v.end("cut")
	}
	return len(vs)
}

// Forget drops port's cached answer (a new lease issued on it).
func (s *Server) Forget(port int) {
	s.mu.Lock()
	delete(s.cache, port)
	s.mu.Unlock()
}

// Visits is the number of open visits of a lease.
func (s *Server) Visits(shareID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.visits[shareID])
}

func (s *Server) forget(v *visit) {
	s.mu.Lock()
	delete(s.all, v)
	if m := s.visits[v.g.ShareID]; m != nil {
		delete(m, v)
		if len(m) == 0 {
			delete(s.visits, v.g.ShareID)
		}
	}
	s.mu.Unlock()
}

// visit is an admitted visitor's connection: it counts bytes and activity
// and is closed once, with the first reason.
type visit struct {
	net.Conn
	g           Grant
	in, out     atomic.Int64
	last        atomic.Int64 // unix nanos of the last byte either way
	idle        time.Duration
	tm          sync.Mutex // guards the timers, set while their callbacks may run
	hard, idler *time.Timer
	why         atomic.Pointer[string]
}

func (v *visit) Read(p []byte) (int, error) {
	n, err := v.Conn.Read(p)
	if n > 0 {
		v.in.Add(int64(n))
		v.last.Store(time.Now().UnixNano())
	}
	return n, err
}

func (v *visit) Write(p []byte) (int, error) {
	n, err := v.Conn.Write(p)
	if n > 0 {
		v.out.Add(int64(n))
		v.last.Store(time.Now().UnixNano())
	}
	return n, err
}

// CloseWrite half-closes when the visitor's connection can.
func (v *visit) CloseWrite() error {
	if cw, ok := v.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return v.Conn.Close()
}

func (v *visit) Close() error {
	v.end("closed")
	return nil
}

func (v *visit) end(why string) {
	if !v.why.CompareAndSwap(nil, &why) {
		return
	}
	v.tm.Lock()
	if v.hard != nil {
		v.hard.Stop()
	}
	if v.idler != nil {
		v.idler.Stop()
	}
	v.tm.Unlock()
	_ = v.Conn.Close()
}

func (v *visit) checkIdle() {
	if v.why.Load() != nil {
		return
	}
	left := v.idle - time.Since(time.Unix(0, v.last.Load()))
	if left <= 0 {
		v.end("idle")
		return
	}
	v.tm.Lock()
	v.idler.Reset(left)
	v.tm.Unlock()
}

func sourceIP(c net.Conn) net.IP {
	if a, ok := c.RemoteAddr().(*net.TCPAddr); ok {
		return a.IP
	}
	return nil
}
