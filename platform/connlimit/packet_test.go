// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connlimit

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/nfltr/quic-go"
	"golang.org/x/net/ipv4"

	"github.com/nfltr/relay-core/internal/platform/testcert"
)

// wrappedUDP is a loopback socket and its wrap.
func wrappedUDP(t *testing.T, cfg PacketConfig) (net.PacketConn, *net.UDPConn) {
	t.Helper()
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = u.Close() })
	cfg.Listener = "test"
	return WrapPacketConn(u, cfg), u
}

func send(t *testing.T, to net.Addr, payloads ...string) {
	t.Helper()
	c, err := net.DialUDP("udp", nil, to.(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close() //nolint:errcheck
	for _, p := range payloads {
		if _, err := c.Write([]byte(p)); err != nil {
			t.Fatal(err)
		}
	}
}

// batches is a socket's ReadBatch returning the given batches, then an
// error.
type batches [][]ipv4.Message

func (b *batches) ReadBatch(ms []ipv4.Message, _ int) (int, error) {
	if len(*b) == 0 {
		return 0, net.ErrClosed
	}
	next := (*b)[0]
	*b = (*b)[1:]
	for i, m := range next {
		ms[i].N = copy(ms[i].Buffers[0], m.Buffers[0])
		ms[i].NN = copy(ms[i].OOB, m.OOB)
		ms[i].Addr, ms[i].Flags = m.Addr, m.Flags
	}
	return len(next), nil
}

// TestPacketRateDropsOnlyTheFloodingSource: a source over its rate loses
// its extra datagrams; another source's arrive whole, in order, after
// the dropped ones in the same batch (their slots are reused).
func TestPacketRateDropsOnlyTheFloodingSource(t *testing.T) {
	pc, _ := wrappedUDP(t, PacketConfig{Rate: 0.001, Burst: 3})
	if _, ok := pc.(interface {
		ReadBatch([]ipv4.Message, int) (int, error)
	}); !ok {
		t.Fatal("the wrapped *net.UDPConn has no ReadBatch: quic-go would read around the filter or lose batching")
	}
	flood, other := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}, &net.UDPAddr{IP: net.IPv4(192, 0, 2, 2), Port: 2}
	msg := func(a net.Addr, p string) ipv4.Message {
		return ipv4.Message{Buffers: [][]byte{[]byte(p)}, OOB: []byte("oob-" + p), Addr: a}
	}
	in := batches{{msg(flood, "f0"), msg(flood, "f1"), msg(flood, "f2"), msg(flood, "f3"), msg(other, "o0"), msg(flood, "f4"), msg(other, "o1")},
		{msg(flood, "f5"), msg(flood, "f6")}, // nothing passes: read again
		{msg(other, "o2")}}
	c := pc.(*udpConn)
	c.batch = &in
	ms := make([]ipv4.Message, 8)
	for i := range ms {
		ms[i].Buffers = [][]byte{make([]byte, 1500)}
		ms[i].OOB = make([]byte, 64)
	}
	var got []string
	for {
		n, err := c.ReadBatch(ms, 0)
		if err != nil {
			break
		}
		if n == 0 {
			t.Fatal("ReadBatch returned no datagram and no error: quic-go would take an empty packet")
		}
		for _, m := range ms[:n] {
			got = append(got, string(m.Buffers[0][:m.N])+"/"+string(m.OOB[:m.NN])+"/"+m.Addr.String())
		}
	}
	want := []string{"f0/oob-f0/192.0.2.1:1", "f1/oob-f1/192.0.2.1:1", "f2/oob-f2/192.0.2.1:1",
		"o0/oob-o0/192.0.2.2:2", "o1/oob-o1/192.0.2.2:2", "o2/oob-o2/192.0.2.2:2"}
	if !slices.Equal(got, want) {
		t.Fatalf("read %q, want %q", got, want)
	}
}

// TestPacketRateExemptsLoopbackAndReadFrom: with ExemptLoopback every
// loopback datagram passes; ReadFrom (quic-go without batched reads) is
// filtered as ReadBatch is.
func TestPacketRateExemptsLoopbackAndReadFrom(t *testing.T) {
	for _, exempt := range []bool{true, false} {
		pc, u := wrappedUDP(t, PacketConfig{Rate: 0.001, Burst: 2, ExemptLoopback: exempt})
		send(t, u.LocalAddr(), "a", "b", "c", "d")
		n := 0
		_ = pc.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		for b := make([]byte, 64); ; n++ {
			if _, _, err := pc.ReadFrom(b); err != nil {
				break
			}
		}
		if want := map[bool]int{true: 4, false: 2}[exempt]; n != want {
			t.Fatalf("exempt=%v: read %d datagrams, want %d", exempt, n, want)
		}
	}
}

// TestPacketRateOffIsTheSocket: rate 0 returns the socket itself.
func TestPacketRateOffIsTheSocket(t *testing.T) {
	pc, u := wrappedUDP(t, PacketConfig{})
	if pc != net.PacketConn(u) {
		t.Fatal("rate 0 wrapped the socket")
	}
}

// TestPacketSourcesBounded: past maxSources a new source passes untracked
// (spoofed sources cost a bounded table, never a refusal); IPv6 sources
// count per /64; a sweep forgets quiet ones.
func TestPacketSourcesBounded(t *testing.T) {
	f := &packetFilter{all: bucketConfig{1, 1}, sources: map[netip.Addr]*source{}}
	now := time.Now()
	f.swept = now
	addr := func(i int) net.Addr { return &net.UDPAddr{IP: net.IPv4(10, byte(i>>16), byte(i>>8), byte(i))} }
	for i := range maxSources {
		if !f.admit(addr(i), false, now) {
			t.Fatalf("source %d refused on its first datagram", i)
		}
	}
	if f.admit(addr(0), false, now) {
		t.Fatal("a tracked source over its rate passed")
	}
	for range 3 {
		if !f.admit(addr(maxSources), false, now) {
			t.Fatal("a source past the table's bound was refused")
		}
	}
	if len(f.sources) != maxSources {
		t.Fatalf("%d sources tracked, want the bound %d", len(f.sources), maxSources)
	}
	f.admit(addr(1), false, now.Add(2*time.Minute)) // sweeps first
	if len(f.sources) != 1 {
		t.Fatalf("%d sources after a sweep of quiet ones, want 1", len(f.sources))
	}
	f = &packetFilter{all: bucketConfig{0.001, 1}, sources: map[netip.Addr]*source{}, swept: now}
	if !f.admit(&net.UDPAddr{IP: net.ParseIP("2001:db8::1")}, false, now) || f.admit(&net.UDPAddr{IP: net.ParseIP("2001:db8::2")}, false, now) {
		t.Fatal("two addresses of one IPv6 /64 did not share a bucket")
	}
}

// TestPacketHandshakeRate: long-header datagrams (QUIC handshakes and
// version negotiation) spend their own, smaller bucket; short headers of
// the same source still pass.
func TestPacketHandshakeRate(t *testing.T) {
	now := time.Now()
	f := &packetFilter{all: bucketConfig{0.001, 100}, long: bucketConfig{0.001, 2}, sources: map[netip.Addr]*source{}, swept: now}
	a := &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1)}
	var got []bool
	for _, long := range []bool{true, true, true, false, true, false} {
		got = append(got, f.admit(a, long, now))
	}
	if want := []bool{true, true, false, true, false, true}; !slices.Equal(got, want) {
		t.Fatalf("admitted %v, want %v", got, want)
	}
	if !longHeader([]byte{0xc3}) || longHeader([]byte{0x43}) || longHeader(nil) {
		t.Fatal("longHeader misreads the first bit")
	}
}

// TestPacketHandshakeFitsItsBurst: a whole QUIC handshake (quic-go's,
// with its post-quantum ClientHello) passes a source whose long-header
// bucket holds 8 datagrams and does not refill, so the burst a source is
// given counts handshakes at about this many datagrams each.
func TestPacketHandshakeFitsItsBurst(t *testing.T) {
	cert := testcert.Generate(t)
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	pc := WrapPacketConn(u, PacketConfig{Listener: "test", HandshakeRate: 1e-9, HandshakeBurst: 8})
	tr := &quic.Transport{Conn: pc}
	defer tr.Close() //nolint:errcheck
	ln, err := tr.Listen(&tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	accepted := make(chan error, 1)
	go func() {
		q, err := ln.Accept(ctx)
		if err == nil {
			_ = q.CloseWithError(0, "")
		}
		accepted <- err
	}()
	q, err := quic.DialAddr(ctx, u.LocalAddr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"t"}}, nil) //nolint:gosec // test-only self-signed cert
	if err != nil {
		t.Fatalf("a handshake through 8 long-header datagrams: %v", err)
	}
	_ = q.CloseWithError(0, "")
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
}
