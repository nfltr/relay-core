// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package connlimit refuses connections per source IP at accept, before the
// relay reads a byte: no SNI peek, TLS handshake, goroutine or buffer is
// spent on a connection over its IP's limits. Two limits, each off at 0:
// connections an IP holds open at once (MaxPerIP) and new connections an IP
// opens a second (Rate, bursts up to Burst). A refused connection is closed
// and counted (metrics.IncrConnRefusals).
// docs/decisions/relay-flood-hardening-2026-11-05.md (nfltr) has the
// measurements behind the defaults.
package connlimit

import (
	"net"
	"sync"
	"time"

	"github.com/nfltr/relay-core/platform/metrics"
	"github.com/nfltr/relay-core/platform/ratelimit"
)

// Refusal reasons, the metric's reason label.
const (
	ReasonPerIPConns = "per_ip_conns"
	ReasonPerIPRate  = "per_ip_rate"
)

// Config is one listener's limits.
type Config struct {
	Listener       string  // the metric's listener label
	MaxPerIP       int     // open connections per source IP; 0: no limit
	Rate           float64 // new connections a second per source IP; 0: no limit
	Burst          int     // Rate's burst
	ExemptLoopback bool    // loopback peers (this machine's own services) pass
}

// Wrap returns l with cfg's limits, or l itself when cfg limits nothing.
func Wrap(l net.Listener, cfg Config) net.Listener {
	if cfg.MaxPerIP <= 0 && cfg.Rate <= 0 {
		return l
	}
	w := &listener{Listener: l, cfg: cfg, open: map[string]int{}}
	if cfg.Rate > 0 {
		// ponytail: a bucket per address; an IPv6 attacker with a /64 gets
		// one per address, group by prefix if that is seen.
		w.rate = ratelimit.New(cfg.Rate, max(cfg.Burst, 1), time.Minute, 5*time.Minute)
	}
	return w
}

type listener struct {
	net.Listener
	cfg  Config
	rate *ratelimit.Limiter
	mu   sync.Mutex
	open map[string]int // admitted connections not yet closed, by IP
}

// Accept returns the next connection within its IP's limits; it closes the
// others itself and keeps accepting.
func (l *listener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if admitted, ok := l.admit(c); ok {
			return admitted, nil
		}
	}
}

func (l *listener) admit(c net.Conn) (net.Conn, bool) {
	ip := sourceIP(c.RemoteAddr())
	if ip == nil || (l.cfg.ExemptLoopback && ip.IsLoopback()) {
		return c, true
	}
	key := ip.String()
	reason := ""
	l.mu.Lock()
	switch {
	case l.cfg.MaxPerIP > 0 && l.open[key] >= l.cfg.MaxPerIP:
		reason = ReasonPerIPConns
	case l.rate != nil && !l.rate.Allow(key):
		reason = ReasonPerIPRate
	default:
		l.open[key]++
	}
	l.mu.Unlock()
	if reason != "" {
		metrics.IncrConnRefusals(l.cfg.Listener, reason)
		if tc, ok := c.(*net.TCPConn); ok {
			_ = tc.SetLinger(0) // RST: no TIME_WAIT kept for a refused flood
		}
		_ = c.Close()
		return nil, false
	}
	release := func() {
		l.mu.Lock()
		if l.open[key]--; l.open[key] <= 0 {
			delete(l.open, key)
		}
		l.mu.Unlock()
	}
	if tc, ok := c.(*net.TCPConn); ok {
		return &tcpConn{TCPConn: tc, release: sync.OnceFunc(release)}, true
	}
	return &conn{Conn: c, release: sync.OnceFunc(release)}, true
}

// tcpConn keeps *net.TCPConn's methods (CloseWrite, SetLinger, ReadFrom)
// that the relay's routes assert on; Close frees the IP's slot once.
type tcpConn struct {
	*net.TCPConn
	release func()
}

func (c *tcpConn) Close() error { c.release(); return c.TCPConn.Close() }

type conn struct {
	net.Conn
	release func()
}

func (c *conn) Close() error { c.release(); return c.Conn.Close() }

func sourceIP(a net.Addr) net.IP {
	if ta, ok := a.(*net.TCPAddr); ok {
		return ta.IP
	}
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return nil
	}
	return net.ParseIP(host)
}
