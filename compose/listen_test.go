// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"errors"
	"net"
	"slices"
	"strconv"
	"testing"

	"github.com/nfltr/relay-core/platform/config"
	"github.com/spf13/viper"
)

// A relay started with every port at 0 binds each one itself: the ports are
// distinct, held by the listeners it serves on (nobody else can bind them),
// and recorded in the config so BoundPorts reports the real ports. The old
// launcher probed ports, closed the probes and passed the numbers on, so
// another process could take one before the relay bound it.
func TestListenBindsZeroPortsItselfAndReportsThem(t *testing.T) {
	viper.Set("http.server.enable", true)
	viper.Set("rpc.server.enable", true)
	viper.Set("internal.enable", true)
	for _, key := range []string{config.ProxyServerPort, config.GrpcServerPort, "internal.port", "metrics.port"} {
		viper.Set(key, "0")
	}
	viper.Set("internal.address", "127.0.0.1")
	defer viper.Reset()

	listeners, _, err := Listen(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	bound := BoundPorts(false)
	// A sync relay binds no dispatcher port: peers' HTTP requests ride the
	// replica link (link phase 3b).
	if len(bound) != 4 || len(listeners) != 4 {
		t.Fatalf("bound=%v listeners=%d, want 4 distinct ports (proxy, gRPC, internal, metrics)", bound, len(listeners))
	}
	for key, port := range bound {
		l := listeners[port]
		if l == nil || port == "0" {
			t.Fatalf("%s=%s has no listener", key, port)
		}
		if got := strconv.Itoa(l.Addr().(*net.TCPAddr).Port); got != port {
			t.Fatalf("%s: config says %s, listener is on %s", key, port, got)
		}
		if other, err := net.Listen("tcp", l.Addr().String()); err == nil {
			_ = other.Close()
			t.Fatalf("%s: port %s was free to another listener", key, port)
		}
	}
	if host := listeners[bound["internal.port"]].Addr().(*net.TCPAddr).IP.String(); host != "127.0.0.1" {
		t.Fatalf("internal port bound on %s, want internal.address", host)
	}
}

// A pinned port is bound as configured next to ephemeral ones.
func TestListenKeepsPinnedPorts(t *testing.T) {
	pin, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pinned := strconv.Itoa(pin.Addr().(*net.TCPAddr).Port)
	_ = pin.Close()
	viper.Set("http.server.enable", true)
	viper.Set(config.ProxyServerPort, pinned)
	viper.Set("rpc.server.enable", false)
	viper.Set("internal.enable", false)
	viper.Set("metrics.port", "0")
	viper.Set("internal.address", "127.0.0.1")
	defer viper.Reset()

	listeners, _, err := Listen(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	if bound := BoundPorts(false); bound[config.ProxyServerPort] != pinned || bound["metrics.port"] == "0" || listeners[pinned] == nil || len(listeners) != 2 {
		t.Fatalf("bound=%v listeners=%d", bound, len(listeners))
	}
}

// listen.address binds the proxy and gRPC ports on that address only (a
// relay on one interface); unset they bind every interface.
func TestListenHonoursListenAddress(t *testing.T) {
	for _, tc := range []struct{ address, want string }{
		{"127.0.0.1", "127.0.0.1"},
		{"", ""}, // every interface
	} {
		viper.Set("http.server.enable", true)
		viper.Set("rpc.server.enable", true)
		viper.Set("internal.enable", false)
		viper.Set(config.ProxyServerPort, "0")
		viper.Set(config.GrpcServerPort, "0")
		viper.Set("metrics.port", "0")
		viper.Set("internal.address", "127.0.0.1")
		viper.Set("listen.address", tc.address)
		listeners, _, err := Listen(false)
		if err != nil {
			t.Fatal(err)
		}
		bound := BoundPorts(false)
		for _, key := range []string{config.ProxyServerPort, config.GrpcServerPort} {
			ip := listeners[bound[key]].Addr().(*net.TCPAddr).IP
			if (tc.want == "" && !ip.IsUnspecified()) || (tc.want != "" && ip.String() != tc.want) {
				t.Errorf("listen.address=%q: %s bound on %s, want %q (empty: every interface)", tc.address, key, ip, tc.want)
			}
		}
		for _, l := range listeners {
			_ = l.Close()
		}
		viper.Reset()
	}
}

// A comma-separated listen.address binds the proxy and gRPC ports on every
// entry, on one port each, served through one listener (the local relay adds
// the Docker bridge gateway to loopback this way).
func TestListenBindsEveryListenAddressOnOnePort(t *testing.T) {
	if l, err := net.Listen("tcp", "[::1]:0"); err != nil {
		t.Skipf("no IPv6 loopback here: %v", err)
	} else {
		_ = l.Close()
	}
	viper.Set("http.server.enable", true)
	viper.Set("rpc.server.enable", true)
	viper.Set("internal.enable", false)
	viper.Set(config.ProxyServerPort, "0")
	viper.Set(config.GrpcServerPort, "0")
	viper.Set("metrics.port", "0")
	viper.Set("internal.address", "127.0.0.1")
	viper.Set("listen.address", "127.0.0.1, ::1")
	defer viper.Reset()
	listeners, _, err := Listen(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	bound := BoundPorts(false)
	for _, key := range []string{config.ProxyServerPort, config.GrpcServerPort} {
		l := listeners[bound[key]]
		if ip := l.Addr().(*net.TCPAddr).IP.String(); ip != "127.0.0.1" {
			t.Fatalf("%s Addr = %s, want the first address", key, ip)
		}
		for _, host := range []string{"127.0.0.1", "::1"} {
			c, err := net.Dial("tcp", net.JoinHostPort(host, bound[key]))
			if err != nil {
				t.Fatalf("%s not listening on %s: %v", key, host, err)
			}
			accepted, err := l.Accept()
			if err != nil {
				t.Fatalf("%s accept from %s: %v", key, host, err)
			}
			_ = accepted.Close()
			_ = c.Close()
		}
	}
	l := listeners[bound[config.ProxyServerPort]]
	_ = l.Close()
	if _, err := l.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
	}
	if c, err := net.Dial("tcp", net.JoinHostPort("::1", bound[config.ProxyServerPort])); err == nil {
		_ = c.Close()
		t.Fatal("second address still listening after Close")
	}
}

// forget undoes the config override Listen set for its kernel-chosen ports:
// the next Listen in this process asks the kernel again instead of binding
// the previous run's numbers as fixed ports (an in-process restart then
// failed with "address already in use" once anything else held one).
func TestListenForgetReadsTheConfiguredPortsAgain(t *testing.T) {
	viper.Set("http.server.enable", false)
	viper.Set("rpc.server.enable", false)
	viper.Set("internal.enable", false)
	viper.SetDefault("metrics.port", "0") // a layer below Set, as env and flags are
	viper.Set("internal.address", "127.0.0.1")
	defer viper.Reset()

	listeners, forget, err := Listen(false)
	if err != nil {
		t.Fatal(err)
	}
	port := viper.GetString("metrics.port")
	if port == "0" || listeners[port] == nil {
		t.Fatalf("metrics.port=%s listeners=%v, want the bound port", port, listeners)
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	forget()
	if got := viper.GetString("metrics.port"); got != "0" {
		t.Fatalf("after forget metrics.port=%s, want 0 (the configured value)", got)
	}
	held, err := net.Listen("tcp", "127.0.0.1:"+port) // someone else takes it
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close() //nolint:errcheck
	again, forget, err := Listen(false)
	if err != nil {
		t.Fatalf("second Listen with the first run's port held: %v", err)
	}
	forget()
	for _, l := range again {
		_ = l.Close()
	}
}

// The SNI router's port (E2EE passthrough on a port of its own: tcp sessions
// and no TLS on the gRPC port) is bound by Listen like the others: "0" binds
// a kernel-chosen port, held for the frontend to serve and reported by
// BoundPorts (the ports file), and forget reads the configured "0" again.
// The frontend bound it itself after the ports file was written, so a
// launcher had to probe a port, close it and pass its number on, and the
// relay failed to start when another process took it in between.
func TestListenBindsZeroSNIPortAndReportsIt(t *testing.T) {
	viper.Set("http.server.enable", false)
	viper.Set("rpc.server.enable", true)
	viper.SetDefault(config.GrpcServerPort, "0") // defaults: forget then reads "0" again
	viper.Set("internal.enable", false)
	viper.SetDefault("metrics.port", "0")
	viper.Set("internal.address", "127.0.0.1")
	viper.Set("listen.address", "127.0.0.1")
	viper.Set("tcp.enabled", true)
	viper.Set("tls.enable", false)
	viper.Set("e2ee.tls_passthrough.enabled", true)
	viper.Set("e2ee.tls_passthrough.base_domain", "e2ee.example.com")
	viper.SetDefault(config.SNIRouterPort, "0") // a layer below Set, as env and flags are
	defer viper.Reset()

	listeners, forget, err := Listen(false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()
	port := BoundPorts(false)[config.SNIRouterPort]
	l := listeners[port]
	if port == "" || port == "0" || l == nil {
		t.Fatalf("BoundPorts=%v, want %s bound to a kernel-chosen port", BoundPorts(false), config.SNIRouterPort)
	}
	if got := strconv.Itoa(l.Addr().(*net.TCPAddr).Port); got != port {
		t.Fatalf("config says %s, listener is on %s", port, got)
	}
	if other, err := net.Listen("tcp", l.Addr().String()); err == nil {
		_ = other.Close()
		t.Fatalf("SNI port %s was free to another listener", port)
	}
	forget()
	if got := viper.GetString(config.SNIRouterPort); got != "0" {
		t.Fatalf("after forget %s=%s, want 0 (the configured value)", config.SNIRouterPort, got)
	}

	// With the SNI mux on the gRPC port (nfltr-relay under tls.enable) it
	// serves E2EE there: no port of its own is bound.
	again, forget, err := Listen(true)
	if err != nil {
		t.Fatal(err)
	}
	forget()
	for _, l := range again {
		_ = l.Close()
	}
	if port, ok := BoundPorts(true)[config.SNIRouterPort]; ok || len(again) != 2 {
		t.Fatalf("with TLS: SNI port %q bound, %d listeners; want none, the SNI mux sits on the gRPC port", port, len(again))
	}
}

func TestPortNumbers(t *testing.T) {
	viper.Set("http.server.enable", true)
	viper.Set(config.ProxyServerPort, "8080")
	viper.Set("rpc.server.enable", true)
	viper.Set(config.GrpcServerPort, "9090")
	viper.Set("internal.enable", true)
	viper.Set("internal.port", "8081")
	viper.Set("metrics.port", "8082")
	defer viper.Reset()

	ports := PortNumbers(false)
	if want := []string{"8080", "9090", "8081", "8082"}; !slices.Equal(ports, want) {
		t.Fatalf("GetPorts = %v, want %v (no dispatcher port)", ports, want)
	}
}

func TestPortNumbers_Minimal(t *testing.T) {
	viper.Set("http.server.enable", false)
	viper.Set("rpc.server.enable", false)
	viper.Set("internal.enable", false)
	viper.Set("metrics.port", "8082")
	defer viper.Reset()

	ports := PortNumbers(false)
	if len(ports) < 1 {
		t.Fatal("expected at least 1 port")
	}
}

func TestListenHostBindsPrivatePortsToTheirAddress(t *testing.T) {
	viper.Set(config.ProxyServerPort, "8080")
	viper.Set(config.GrpcServerPort, "9090")
	viper.Set("internal.port", "8081")
	viper.Set("metrics.port", "8082")
	viper.Set("internal.address", "127.0.0.1")
	defer viper.Reset()

	for port, want := range map[string]string{"8080": "", "9090": "", "8081": "127.0.0.1", "8082": "127.0.0.1"} {
		if got := ListenHost(port); got != want {
			t.Errorf("ListenHost(%s) = %q, want %q", port, got, want)
		}
	}
}
