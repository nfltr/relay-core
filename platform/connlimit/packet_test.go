// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connlimit

import (
	"context"
	"crypto/tls"
	"net"
	"net/netip"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/quic-go"
	"github.com/nfltr/quic-go/qlog"
	"github.com/nfltr/quic-go/qlogwriter"
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

// handshakeDatagrams bounds the long-header datagrams one QUIC handshake
// (quic-go's, with its post-quantum ClientHello) sends the server when
// nothing is retransmitted: 3 on loopback (the ClientHello's two
// Initials, then the Finished with its ACKs), up to 5 when the client
// ACKs the server's flight in datagrams of their own. The handshake burst
// counts handshakes at 3 to this many datagrams each (the flags' help in
// platform/config). A retransmission (a PTO or a loss on either side, as
// on a slow or lossy path) sends more, which the bucket's rate refills.
const handshakeDatagrams = 8

// retransmits records whether a connection's loss recovery sent anything
// again (a PTO fired or a packet was declared lost).
type retransmits struct{ seen atomic.Bool }

func (r *retransmits) AddProducer() qlogwriter.Recorder { return r }
func (r *retransmits) SupportsSchemas(string) bool      { return true }
func (r *retransmits) Close() error                     { return nil }
func (r *retransmits) RecordEvent(e qlogwriter.Event) {
	switch e := e.(type) {
	case qlog.PTOCountUpdated:
		if e.PTOCount > 0 {
			r.seen.Store(true)
		}
	case qlog.PacketLost:
		r.seen.Store(true)
	}
}

// TestPacketHandshakeFitsItsBurst: the long-header datagrams a client
// sends the server in one handshake, counted by the filter itself (a
// bucket that never refills, too large to refuse any), are at most
// handshakeDatagrams. A handshake whose loss recovery retransmitted
// (seen by qlog on either side: a loaded machine) is not the claim and
// is measured again, so the count is the handshake's own, not the
// machine's load.
func TestPacketHandshakeFitsItsBurst(t *testing.T) {
	cert := testcert.Generate(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute) // a hang bound only
	defer cancel()
	for attempt := 1; ; attempt++ {
		n, retransmitted := handshakeLongDatagrams(ctx, t, cert)
		if !retransmitted {
			t.Logf("a handshake sent %d long-header datagrams", n)
			if n == 0 || n > handshakeDatagrams {
				t.Fatalf("a handshake sent %d long-header datagrams, want 1..%d: resize the handshake burst", n, handshakeDatagrams)
			}
			return
		}
		t.Logf("attempt %d retransmitted (%d long-header datagrams): measuring again", attempt, n)
		if attempt == 20 {
			t.Fatal("no handshake without a retransmission in 20 attempts")
		}
	}
}

// handshakeLongDatagrams runs one handshake over loopback and returns the
// long-header datagrams the server read from the client up to it, and
// whether either side retransmitted.
func handshakeLongDatagrams(ctx context.Context, t *testing.T, cert tls.Certificate) (int, bool) {
	t.Helper()
	const burst = 1 << 20
	u, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	pc := WrapPacketConn(u, PacketConfig{Listener: "test", HandshakeRate: 1e-9, HandshakeBurst: burst})
	tr := &quic.Transport{Conn: pc}
	defer tr.Close() //nolint:errcheck
	var srv, cli retransmits
	ln, err := tr.Listen(&tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"t"}, MinVersion: tls.VersionTLS13},
		&quic.Config{Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return &srv }})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close() //nolint:errcheck
	accepted := make(chan *quic.Conn, 1)
	go func() {
		q, err := ln.Accept(ctx)
		if err != nil {
			t.Error(err)
		}
		accepted <- q
	}()
	q, err := quic.DialAddr(ctx, u.LocalAddr().String(), &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"t"}}, //nolint:gosec // test-only self-signed cert
		&quic.Config{Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return &cli }})
	if err != nil {
		t.Fatal(err)
	}
	defer q.CloseWithError(0, "") //nolint:errcheck
	sq := <-accepted              // the server read the client's Finished: the handshake's datagrams are counted
	if sq == nil {
		t.FailNow()
	}
	defer sq.CloseWithError(0, "") //nolint:errcheck
	f := pc.(*udpConn).f
	f.mu.Lock()
	s := f.sources[netip.MustParseAddr("127.0.0.1")]
	n := burst - int(s.long.tokens+0.5)
	f.mu.Unlock()
	return n, srv.seen.Load() || cli.seen.Load()
}
