// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connlimit

import (
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/nfltr/relay-core/platform/metrics"
	"golang.org/x/net/ipv4"
)

// ReasonPerIPPackets is the refusal reason of a datagram over its source's
// packet rate.
const ReasonPerIPPackets = "per_ip_packets"

// PacketConfig is one QUIC socket's limits per source (IPv4 address, IPv6
// /64): datagrams a second (Rate, bursts up to Burst) and, of those,
// long-header datagrams a second (HandshakeRate, HandshakeBurst). QUIC
// sends long headers only while a connection is set up (Initial, 0-RTT,
// Handshake; a few each) and in version negotiation: each Initial with a
// new connection ID makes the server start a handshake that it holds
// until the handshake timeout, so their rate bounds a source's
// half-open connections as the TCP connection rate does. A rate 0 turns
// its limit off.
type PacketConfig struct {
	Listener       string // the metric's listener label
	Rate           float64
	Burst          int
	HandshakeRate  float64
	HandshakeBurst int
	ExemptLoopback bool
}

// maxSources bounds the sources tracked at once: past it a new source
// passes untracked, so spoofed random sources cost a bounded table and
// never refuse a real one.
const maxSources = 1 << 16

// WrapPacketConn returns pc dropping, before the caller reads them, the
// datagrams of a source over cfg's rate; pc itself when cfg limits
// nothing. A *net.UDPConn keeps its methods (quic-go's batched reads, GSO,
// ECN): reads through ReadBatch and ReadFrom are filtered; the batched
// read is what quic-go uses on Linux, BSD and macOS.
func WrapPacketConn(pc net.PacketConn, cfg PacketConfig) net.PacketConn {
	if cfg.Rate <= 0 && cfg.HandshakeRate <= 0 {
		return pc
	}
	f := &packetFilter{cfg: cfg, sources: map[netip.Addr]*source{},
		all: bucketConfig{cfg.Rate, float64(max(cfg.Burst, 1))}, long: bucketConfig{cfg.HandshakeRate, float64(max(cfg.HandshakeBurst, 1))}}
	if u, ok := pc.(*net.UDPConn); ok {
		return &udpConn{UDPConn: u, f: f, batch: ipv4.NewPacketConn(u)}
	}
	return &packetConn{PacketConn: pc, f: f}
}

type bucketConfig struct{ rate, burst float64 }

type bucket struct {
	tokens float64
	last   time.Time
}

// take spends a token if the bucket has one (rate 0: always).
func (b *bucket) take(c bucketConfig, now time.Time) bool {
	if c.rate <= 0 {
		return true
	}
	b.tokens = min(c.burst, b.tokens+now.Sub(b.last).Seconds()*c.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// full reports whether the bucket refilled to its burst by now.
func (b *bucket) full(c bucketConfig, now time.Time) bool {
	return c.rate <= 0 || b.tokens+now.Sub(b.last).Seconds()*c.rate >= c.burst
}

type source struct{ all, long bucket }

// packetFilter keeps two token buckets per source. One goroutine reads a
// socket, but the lock keeps it safe for more.
type packetFilter struct {
	cfg       PacketConfig
	all, long bucketConfig
	mu        sync.Mutex
	sources   map[netip.Addr]*source
	swept     time.Time
}

// admit reports whether a datagram from a passes, spending its tokens;
// long tells a QUIC long header (first bit set).
func (f *packetFilter) admit(a net.Addr, long bool, now time.Time) bool {
	ua, ok := a.(*net.UDPAddr)
	if !ok {
		return true
	}
	ip, ok := netip.AddrFromSlice(ua.IP)
	if !ok {
		return true
	}
	ip = ip.Unmap()
	if f.cfg.ExemptLoopback && ip.IsLoopback() {
		return true
	}
	if ip.Is6() {
		ip = netip.PrefixFrom(ip, 64).Masked().Addr()
	}
	if now.Sub(f.swept) > time.Minute {
		f.sweep(now)
	}
	s := f.sources[ip]
	if s == nil {
		if len(f.sources) >= maxSources {
			return true
		}
		s = &source{all: bucket{f.all.burst, now}, long: bucket{f.long.burst, now}}
		f.sources[ip] = s
	}
	if long && !s.long.take(f.long, now) {
		return false
	}
	return s.all.take(f.all, now)
}

// sweep forgets sources whose buckets are full (quiet for a burst's time).
func (f *packetFilter) sweep(now time.Time) {
	f.swept = now
	for ip, s := range f.sources {
		if s.all.full(f.all, now) && s.long.full(f.long, now) {
			delete(f.sources, ip)
		}
	}
}

// longHeader reports whether a datagram starts with a QUIC long header.
func longHeader(b []byte) bool { return len(b) > 0 && b[0]&0x80 != 0 }

func (f *packetFilter) refused(n int) {
	if n > 0 {
		metrics.AddConnRefusals(f.cfg.Listener, ReasonPerIPPackets, n)
	}
}

// udpConn is a filtered *net.UDPConn: its methods stay (quic-go asserts on
// them); ReadBatch and ReadFrom drop over-rate datagrams.
type udpConn struct {
	*net.UDPConn
	f     *packetFilter
	batch interface { // the socket's ipv4.PacketConn
		ReadBatch(ms []ipv4.Message, flags int) (int, error)
	}
}

// ReadBatch reads datagrams and keeps those within their source's rate at
// the front of ms (copied into the slots of the dropped ones, which keep
// their own buffers); it reads again while a batch has none.
func (c *udpConn) ReadBatch(ms []ipv4.Message, flags int) (int, error) {
	for {
		n, err := c.batch.ReadBatch(ms, flags)
		if n == 0 || err != nil {
			return n, err
		}
		now := time.Now()
		k := 0
		c.f.mu.Lock()
		for i := range n {
			if !c.f.admit(ms[i].Addr, longHeader(ms[i].Buffers[0][:ms[i].N]), now) {
				continue
			}
			if k != i {
				d, s := &ms[k], &ms[i]
				d.N = copy(d.Buffers[0], s.Buffers[0][:s.N])
				d.NN = copy(d.OOB, s.OOB[:s.NN])
				d.Flags, d.Addr = s.Flags, s.Addr
			}
			k++
		}
		c.f.mu.Unlock()
		c.f.refused(n - k)
		if k > 0 {
			return k, nil
		}
	}
}

func (c *udpConn) ReadFrom(b []byte) (int, net.Addr, error) { return readFrom(c.UDPConn, c.f, b) }

type packetConn struct {
	net.PacketConn
	f *packetFilter
}

func (c *packetConn) ReadFrom(b []byte) (int, net.Addr, error) { return readFrom(c.PacketConn, c.f, b) }

func readFrom(pc net.PacketConn, f *packetFilter, b []byte) (int, net.Addr, error) {
	for {
		n, a, err := pc.ReadFrom(b)
		if err != nil {
			return n, a, err
		}
		f.mu.Lock()
		ok := f.admit(a, longHeader(b[:n]), time.Now())
		f.mu.Unlock()
		if ok {
			return n, a, nil
		}
		f.refused(1)
	}
}
