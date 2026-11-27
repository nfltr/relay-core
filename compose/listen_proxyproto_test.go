// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"bufio"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/nfltr/relay-core/platform/config"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/spf13/viper"
)

// listenGRPC binds the gRPC port with security.proxy_protocol.trusted set to
// trusted and the per-source connection cap at 1; it returns the port's
// listener and its address.
func listenGRPC(t *testing.T, trusted string) (net.Listener, string) {
	t.Helper()
	viper.Set("grpc.enabled", true)
	viper.Set(config.GrpcServerPort, "0")
	viper.Set("metrics.port", "0")
	viper.Set("internal.address", "127.0.0.1")
	viper.Set("listen.address", "127.0.0.1")
	viper.Set("security.conn_limit.per_ip_max", 1)
	viper.Set("security.proxy_protocol.trusted", trusted)
	t.Cleanup(viper.Reset)
	listeners, _, err := Listen(false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	})
	lastListeners = listeners
	l := listeners[viper.GetString(config.GrpcServerPort)]
	return l, l.Addr().String()
}

// lastListeners: listenGRPC's, for listenedPort.
var lastListeners map[string]net.Listener

// listenedPort is listenGRPC's listener of the port config key names.
func listenedPort(t *testing.T, key string) net.Listener {
	t.Helper()
	l := lastListeners[viper.GetString(key)]
	if l == nil {
		t.Fatalf("no listener for %s", key)
	}
	return l
}

// dialWithHeader connects to addr and sends a PROXY protocol v2 header naming
// src as the client, then line.
func dialWithHeader(t *testing.T, addr, src, line string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if src != "" {
		h := proxyproto.HeaderProxyFromAddrs(2, net.TCPAddrFromAddrPort(netip.MustParseAddrPort(src)), net.TCPAddrFromAddrPort(netip.MustParseAddrPort("198.51.100.1:443")))
		if _, err := h.WriteTo(c); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Write([]byte(line + "\n")); err != nil {
		t.Fatal(err)
	}
	return c
}

type accepted struct {
	conn net.Conn
	err  error
}

func acceptOne(t *testing.T, l net.Listener) chan accepted {
	ch := make(chan accepted, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			t.Cleanup(func() { _ = c.Close() })
		}
		ch <- accepted{c, err}
	}()
	return ch
}

func readLine(t *testing.T, c net.Conn) (string, error) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	return bufio.NewReader(c).ReadString('\n')
}

// Behind a load balancer (lane SCALE-LB): a connection from a trusted sender
// carries a PROXY protocol v2 header, and the relay sees the client it names
// — the per-source connection cap counts that client, not the balancer, and
// the stream starts after the header.
func TestListenTakesTheClientFromATrustedProxyHeader(t *testing.T) {
	l, addr := listenGRPC(t, "127.0.0.0/8")
	ch := acceptOne(t, l)
	dialWithHeader(t, addr, "203.0.113.7:5000", "hello")
	a := <-ch
	if a.err != nil {
		t.Fatal(a.err)
	}
	if got := a.conn.RemoteAddr().String(); got != "203.0.113.7:5000" {
		t.Fatalf("RemoteAddr = %s, want the header's client 203.0.113.7:5000", got)
	}
	if line, err := readLine(t, a.conn); err != nil || line != "hello\n" {
		t.Fatalf("first line = %q, %v; want the stream after the header", line, err)
	}
	// The cap is 1 per source: a second connection of the same client is
	// refused, one of another client is admitted (loopback, the balancer's
	// address here, would be exempt from the cap).
	dialWithHeader(t, addr, "203.0.113.7:5001", "again")
	dialWithHeader(t, addr, "203.0.113.8:5000", "other")
	b := <-acceptOne(t, l)
	if b.err != nil {
		t.Fatal(b.err)
	}
	if got := b.conn.RemoteAddr().String(); got != "203.0.113.8:5000" {
		t.Fatalf("admitted %s, want 203.0.113.8:5000 (203.0.113.7's second connection is over its cap)", got)
	}
}

// A trusted sender must send the header: a connection from it without one
// carries no client and is never served.
func TestListenRefusesATrustedSenderWithoutHeader(t *testing.T) {
	l, addr := listenGRPC(t, "127.0.0.0/8")
	ch := acceptOne(t, l)
	dialWithHeader(t, addr, "", "no header here")
	a := <-ch
	if a.err != nil {
		t.Fatal(a.err)
	}
	if line, err := readLine(t, a.conn); err == nil {
		t.Fatalf("read %q from a trusted sender's connection without a header, want an error", line)
	}
}

// A sender outside the trusted ranges cannot name a client: its connection
// keeps its own address and a header it sends is not parsed (it reaches the
// TLS or SNI reader, which refuses it).
func TestListenIgnoresTheHeaderOfAnUntrustedSender(t *testing.T) {
	l, addr := listenGRPC(t, "10.0.0.0/8")
	ch := acceptOne(t, l)
	dialWithHeader(t, addr, "203.0.113.7:5000", "hello")
	a := <-ch
	if a.err != nil {
		t.Fatal(a.err)
	}
	if ip := a.conn.RemoteAddr().(*net.TCPAddr).IP; !ip.IsLoopback() {
		t.Fatalf("RemoteAddr = %s, want the untrusted sender's own (loopback) address", ip)
	}
	if line, err := readLine(t, a.conn); err == nil && line == "hello\n" {
		t.Fatal("the untrusted sender's header was consumed, want it left in the stream")
	}
}

// An invalid range fails the start instead of trusting nobody silently.
func TestListenRefusesAnInvalidTrustedRange(t *testing.T) {
	viper.Set("grpc.enabled", true)
	viper.Set(config.GrpcServerPort, "0")
	viper.Set("metrics.port", "0")
	viper.Set("internal.address", "127.0.0.1")
	viper.Set("security.proxy_protocol.trusted", "10.0.0.0/33")
	defer viper.Reset()
	listeners, _, err := Listen(false)
	for _, l := range listeners {
		_ = l.Close()
	}
	if err == nil {
		t.Fatal("Listen accepted security.proxy_protocol.trusted=10.0.0.0/33")
	}
}

// The balancer's health check (its HTTP check of /ready on the internal
// port) carries the PROXY header too when the service sends one (Hetzner,
// seen in lane SCALE-LB's rehearsal: every target unhealthy): the internal
// port reads it from trusted senders like the public ports; the metrics port
// is never wrapped.
func TestListenReadsTheTrustedHeaderOnTheInternalPort(t *testing.T) {
	viper.Set("internal.enabled", true)
	viper.Set("internal.port", "0")
	listenGRPC(t, "127.0.0.0/8")
	internal := listenedPort(t, "internal.port")
	ch := acceptOne(t, internal)
	dialWithHeader(t, internal.Addr().String(), "10.77.0.2:4282", "GET /ready")
	a := <-ch
	if a.err != nil {
		t.Fatal(a.err)
	}
	if line, err := readLine(t, a.conn); err != nil || line != "GET /ready\n" {
		t.Fatalf("first line = %q, %v; want the request after the header", line, err)
	}
	if _, plain := listenedPort(t, "metrics.port").(*net.TCPListener); !plain {
		t.Fatal("the metrics port is wrapped, want it plain")
	}
}
