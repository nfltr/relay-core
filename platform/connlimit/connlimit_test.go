// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package connlimit

import (
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// listen wraps a loopback listener with cfg (loopback not exempt) and
// returns a dial function (its connection, or nil when the relay reset it
// before connect returned, as Linux reports a refusal racing the dial) and
// a channel of the conns it admitted.
func listen(t *testing.T, cfg Config) (dial func() net.Conn, admitted chan net.Conn) {
	t.Helper()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listener = "test"
	l := Wrap(inner, cfg)
	t.Cleanup(func() { _ = l.Close() })
	admitted = make(chan net.Conn, 16)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				close(admitted)
				return
			}
			admitted <- c
		}
	}()
	dial = func() net.Conn {
		c, err := net.Dial("tcp", inner.Addr().String())
		if errors.Is(err, syscall.ECONNRESET) {
			return nil
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	return dial, admitted
}

// refusedByRelay reports whether the relay closed c without a byte (nil:
// reset before the dial returned).
func refusedByRelay(t *testing.T, c net.Conn) bool {
	t.Helper()
	if c == nil {
		return true
	}
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := c.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return false
	}
	return err != nil
}

func expectAdmitted(t *testing.T, admitted chan net.Conn) net.Conn {
	t.Helper()
	select {
	case c := <-admitted:
		return c
	case <-time.After(2 * time.Second):
		t.Fatal("connection not admitted")
		return nil
	}
}

func TestPerIPConnectionCap(t *testing.T) {
	dial, admitted := listen(t, Config{MaxPerIP: 2})
	dial()
	dial()
	first, _ := expectAdmitted(t, admitted), expectAdmitted(t, admitted)
	before := refusals("test", ReasonPerIPConns)
	if !refusedByRelay(t, dial()) {
		t.Fatal("a third connection from one IP was not refused")
	}
	if got := refusals("test", ReasonPerIPConns) - before; got != 1 {
		t.Fatalf("refusal metric moved by %v, want 1", got)
	}
	// Closing an admitted connection frees its slot (twice: counted once).
	_ = first.Close()
	_ = first.Close()
	dial()
	expectAdmitted(t, admitted)
	if !refusedByRelay(t, dial()) {
		t.Fatal("a double Close freed two slots")
	}
}

func TestPerIPAcceptRate(t *testing.T) {
	dial, admitted := listen(t, Config{Rate: 0.001, Burst: 2})
	dial()
	dial()
	expectAdmitted(t, admitted)
	expectAdmitted(t, admitted)
	before := refusals("test", ReasonPerIPRate)
	if !refusedByRelay(t, dial()) {
		t.Fatal("a connection over the IP's rate was not refused")
	}
	if got := refusals("test", ReasonPerIPRate) - before; got != 1 {
		t.Fatalf("refusal metric moved by %v, want 1", got)
	}
}

func TestLoopbackExempt(t *testing.T) {
	dial, admitted := listen(t, Config{MaxPerIP: 1, Rate: 0.001, Burst: 1, ExemptLoopback: true})
	for range 3 {
		dial()
		expectAdmitted(t, admitted)
	}
}

func TestZeroConfigIsTheListener(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	if Wrap(inner, Config{}) != inner {
		t.Fatal("a config with no limit should not wrap the listener")
	}
}

func TestAdmittedConnKeepsTCPMethods(t *testing.T) {
	dial, admitted := listen(t, Config{MaxPerIP: 4})
	dial()
	c := expectAdmitted(t, admitted)
	if _, ok := c.(interface{ CloseWrite() error }); !ok {
		t.Fatal("admitted conn lost CloseWrite")
	}
	if _, ok := c.(interface{ SetLinger(int) error }); !ok {
		t.Fatal("admitted conn lost SetLinger")
	}
}

func TestCloseEndsAccept(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := Wrap(inner, Config{MaxPerIP: 1, Listener: "test"})
	done := make(chan error, 1)
	go func() { _, err := l.Accept(); done <- err }()
	_ = l.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Accept returned a conn after Close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not return after Close")
	}
}

// refusals reads nfltr_relay_proxy_conn_refused_total{listener,reason}.
func refusals(listener, reason string) float64 {
	mfs, _ := prometheus.DefaultGatherer.Gather()
	for _, mf := range mfs {
		if mf.GetName() != "nfltr_relay_proxy_conn_refused_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			if labels["listener"] == listener && labels["reason"] == reason {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// addrConn is a connection from ip (only its address and Close are used).
type addrConn struct {
	net.Conn
	ip string
}

func (c addrConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.ParseIP(c.ip), Port: 4242} }
func (c addrConn) Close() error         { return nil }

// TestIPv6SourceIsSlash64: the addresses of one IPv6 /64 are one source
// (SourceKey), for the open cap and the rate, as for the accounts' caps and
// the packet filter; another /64 is another source.
func TestIPv6SourceIsSlash64(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close() //nolint:errcheck
	for name, cfg := range map[string]Config{"open": {Listener: "test", MaxPerIP: 1}, "rate": {Listener: "test", Rate: 0.001, Burst: 1}} {
		l := Wrap(inner, cfg).(*listener)
		if _, ok := l.admit(addrConn{ip: "2001:db8::1"}); !ok {
			t.Fatalf("%s: the first connection was refused", name)
		}
		if _, ok := l.admit(addrConn{ip: "2001:db8::ffff:2"}); ok {
			t.Fatalf("%s: another address of the same /64 got a source of its own", name)
		}
		if _, ok := l.admit(addrConn{ip: "2001:db8:0:1::1"}); !ok {
			t.Fatalf("%s: another /64 was refused", name)
		}
	}
}
