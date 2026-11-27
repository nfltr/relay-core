// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/connlimit"
	"github.com/nfltr/relay-core/platform/util"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/spf13/viper"
)

// portKeys returns the config keys of the ports a relay listens on (proxy,
// gRPC, internal, metrics, the SNI router's), in PortNumbers order (proxy, gRPC,
// internal, metrics, SNI router). Peers
// reach this replica's agents over the replica link (peer_link.port, its
// own listener). muxOnGRPC: the binary's SNI frontend sits in front of the
// gRPC port's TLS (frontend.Host.WrapGRPCListener), so it binds no port of
// its own.
func portKeys(muxOnGRPC bool) []string {
	var keys []string
	if viper.GetBool("http.server.enabled") {
		keys = append(keys, config.ProxyServerPort)
	}
	if viper.GetBool("grpc.enabled") {
		keys = append(keys, config.GrpcServerPort)
	}
	if viper.GetBool("internal.enabled") {
		keys = append(keys, "internal.port")
	}
	keys = append(keys, "metrics.port")
	if sniRouter(muxOnGRPC) {
		keys = append(keys, config.SNIRouterPort)
	}
	return keys
}

// sniRouter reports whether the SNI frontend serves E2EE passthrough on a
// port of its own (frontend/sni): tcp sessions, passthrough with a base
// domain (what the frontend serves on), and no mux on the gRPC port
// (nfltr-relay's under tls.enabled; the core never has one). Listen binds
// that port, so "0" is a kernel-chosen port the ports file reports.
func sniRouter(muxOnGRPC bool) bool {
	return viper.GetBool("grpc.enabled") && viper.GetBool("tcp.enabled") &&
		viper.GetBool("e2ee.tls_passthrough.enabled") && viper.GetString("e2ee.tls_passthrough.base_domain") != "" &&
		!muxOnGRPC
}

// listenHostForKey is listenHost by config key, for ports not yet known.
func listenHostForKey(key string) string {
	switch key {
	case "internal.port", "metrics.port":
		return viper.GetString("internal.address")
	}
	return publicListenHosts()[0]
}

// publicListenHosts is listen.address split on commas: the addresses the
// proxy and gRPC ports bind, all on one port. Unset, it is [""] (every
// interface).
func publicListenHosts() []string {
	var hosts []string
	for h := range strings.SplitSeq(viper.GetString("listen.address"), ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		return []string{""}
	}
	return hosts
}

// Listen binds every relay port on its listen host (muxOnGRPC: the SNI frontend shares the gRPC port). A port configured as "0"
// binds a kernel-chosen port and the config is set to that port before
// anything else reads it, so the servers, /ready-ports and BoundPorts all see
// the bound port. A launcher that needs free ports passes 0 and reads them
// back (BoundPorts): no other process can take a port between choosing and
// binding it, as it could when the launcher probed a port, closed the probe
// and handed the number to the relay.
//
// forget puts those keys back as they were: the config is process-global, so
// without it the next relay in this process (an in-process restart) would
// bind this run's kernel-chosen ports as fixed ones, and fail with "address
// already in use" once anything else holds one of them
// (TestListenForgetReadsTheConfiguredPortsAgain).
func Listen(muxOnGRPC bool) (listeners map[string]net.Listener, forget func(), err error) {
	ephemeral := map[string]net.Listener{}
	var chosen []string
	forget = func() {
		for _, key := range chosen {
			viper.Set(key, nil) // a nil override reads through to env, flags and defaults again
		}
	}
	closeAll := func() {
		for _, l := range ephemeral {
			_ = l.Close()
		}
		forget()
	}
	for _, key := range portKeys(muxOnGRPC) {
		if viper.GetString(key) != "0" {
			continue
		}
		host := listenHostForKey(key)
		if host == "" {
			host = "0.0.0.0"
		}
		l, err := net.Listen("tcp", net.JoinHostPort(host, "0"))
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("could not create listener for %s: %w", key, err)
		}
		port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
		viper.Set(key, port)
		chosen = append(chosen, key)
		ephemeral[port] = l
	}
	var fixed []string
	for _, port := range PortNumbers(muxOnGRPC) {
		if ephemeral[port] == nil {
			fixed = append(fixed, port)
		}
	}
	listeners, err = util.GetListeners(fixed, listenHost)
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	maps.Copy(listeners, ephemeral)
	if err := listenOnExtraHosts(listeners, portKeys(muxOnGRPC)); err != nil {
		for _, l := range listeners {
			_ = l.Close()
		}
		forget()
		return nil, nil, err
	}
	if err := trustProxyHeaders(listeners, portKeys(muxOnGRPC)); err != nil {
		for _, l := range listeners {
			_ = l.Close()
		}
		forget()
		return nil, nil, err
	}
	limitPublicPorts(listeners, portKeys(muxOnGRPC))
	return listeners, forget, nil
}

// proxyHeaderTimeout bounds the wait for a trusted sender's PROXY header.
// The header is read on the port's accept loop (connlimit asks for the
// source there), so a slow one holds that loop: a load balancer writes it as
// its first bytes. ponytail: serial header reads, trusted senders only; read
// headers concurrently if a sender ever delays them.
const proxyHeaderTimeout = 2 * time.Second

// trustProxyHeaders reads a PROXY protocol (v1 or v2) header on the proxy,
// gRPC, SNI router and internal ports from the senders in
// security.proxy_protocol.trusted (comma-separated IPs or CIDRs: the load
// balancer in front of several relays, lane SCALE-LB), so the per-source
// limits, nf1.source_* caps and rate limits see the client the header names.
// The internal port for the balancer's health check of /ready, which sends
// the header too (Hetzner: PROXY v1 on the check; without it every target
// was unhealthy).
// A trusted sender must send one; any other sender's connection is served
// as it is, its own address its source, a header it sends left unparsed in
// the stream (TLS and the SNI peek refuse it). Empty: no header is read.
func trustProxyHeaders(listeners map[string]net.Listener, keys []string) error {
	var trusted []string
	for r := range strings.SplitSeq(viper.GetString("security.proxy_protocol.trusted"), ",") {
		if r = strings.TrimSpace(r); r != "" {
			trusted = append(trusted, r)
		}
	}
	if len(trusted) == 0 {
		return nil
	}
	policy, err := proxyproto.PolicyFromRanges(trusted, proxyproto.REQUIRE, proxyproto.SKIP)
	if err != nil {
		return fmt.Errorf("security.proxy_protocol.trusted: %w", err)
	}
	for _, key := range keys {
		if key != config.ProxyServerPort && key != config.GrpcServerPort && key != config.SNIRouterPort && key != "internal.port" {
			continue
		}
		port := viper.GetString(key)
		listeners[port] = &proxyproto.Listener{Listener: listeners[port], ConnPolicy: policy, ReadHeaderTimeout: proxyHeaderTimeout}
	}
	return nil
}

// limitPublicPorts puts the per-source-IP connection limits
// (security.conn_limit.*) in front of the proxy, gRPC and SNI router ports,
// before TLS and the SNI peek; the internal and metrics ports stay as they
// are.
func limitPublicPorts(listeners map[string]net.Listener, keys []string) {
	names := map[string]string{config.ProxyServerPort: "proxy", config.GrpcServerPort: "grpc", config.SNIRouterPort: "sni"}
	for _, key := range keys {
		name, public := names[key]
		if !public {
			continue
		}
		port := viper.GetString(key)
		listeners[port] = connlimit.Wrap(listeners[port], connlimit.Config{
			Listener:       name,
			MaxPerIP:       viper.GetInt("security.conn_limit.per_ip_max"),
			Rate:           viper.GetFloat64("security.conn_limit.per_ip_rate"),
			Burst:          viper.GetInt("security.conn_limit.per_ip_burst"),
			ExemptLoopback: true,
		})
	}
}

// listenOnExtraHosts binds the proxy, gRPC and SNI router ports on every listen.address
// entry after the first (already bound there), on the same port, and serves
// them through that port's one listener.
func listenOnExtraHosts(listeners map[string]net.Listener, keys []string) error {
	extra := publicListenHosts()[1:]
	if len(extra) == 0 {
		return nil
	}
	for _, key := range keys {
		if key != config.ProxyServerPort && key != config.GrpcServerPort && key != config.SNIRouterPort {
			continue
		}
		port := viper.GetString(key)
		all := []net.Listener{listeners[port]}
		for _, host := range extra {
			l, err := net.Listen("tcp", net.JoinHostPort(host, port))
			if err != nil {
				for _, opened := range all[1:] {
					_ = opened.Close()
				}
				return fmt.Errorf("could not create listener for %s on %s: %w", key, host, err)
			}
			all = append(all, l)
		}
		listeners[port] = newMultiListener(all)
	}
	return nil
}

// multiListener accepts from several listeners as one; Addr is the first's.
type multiListener struct {
	all    []net.Listener
	conns  chan net.Conn
	errs   chan error
	done   chan struct{}
	closed sync.Once
}

func newMultiListener(all []net.Listener) *multiListener {
	m := &multiListener{all: all, conns: make(chan net.Conn), errs: make(chan error), done: make(chan struct{})}
	for _, l := range all {
		go m.accept(l)
	}
	return m
}

// accept forwards l's connections and errors until l is closed. Other accept
// errors (EMFILE, ECONNABORTED) are passed on and l keeps accepting, as a
// server keeps calling Accept after a temporary error.
func (m *multiListener) accept(l net.Listener) {
	for {
		c, err := l.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			select {
			case m.errs <- err:
				continue
			case <-m.done:
				return
			}
		}
		select {
		case m.conns <- c:
		case <-m.done:
			_ = c.Close()
			return
		}
	}
}

func (m *multiListener) Accept() (net.Conn, error) {
	select {
	case c := <-m.conns:
		return c, nil
	case err := <-m.errs:
		return nil, err
	case <-m.done:
		return nil, net.ErrClosed
	}
}

func (m *multiListener) Close() error {
	var err error
	m.closed.Do(func() {
		close(m.done)
		for _, l := range m.all {
			err = errors.Join(err, l.Close())
		}
	})
	return err
}

// Addr implements net.Listener. allow:wrapper
func (m *multiListener) Addr() net.Addr { return m.all[0].Addr() }

// BoundPorts maps each listened port's config key to its port. After Listen
// these are the bound ports, including ones configured as "0".
func BoundPorts(muxOnGRPC bool) map[string]string {
	ports := map[string]string{}
	for _, key := range portKeys(muxOnGRPC) {
		ports[key] = viper.GetString(key)
	}
	return ports
}

// PortNumbers are the ports a relay listens on, in this order: proxy, gRPC, internal,
// metrics, the SNI router's (each only when enabled).
func PortNumbers(muxOnGRPC bool) []string {
	var ports []string
	for _, key := range portKeys(muxOnGRPC) {
		ports = append(ports, viper.GetString(key))
	}
	return ports
}

// listenHost is the bind address for port: internal.address for the internal
// and metrics ports (admin API, health, /metrics), the first listen.address entry (empty:
// all interfaces) for the public proxy and gRPC listeners (Listen binds the
// other entries too). The standalone deploy binds the first two to loopback.
func listenHost(port string) string {
	switch port {
	case viper.GetString("internal.port"), viper.GetString("metrics.port"):
		return viper.GetString("internal.address")
	}
	return publicListenHosts()[0]
}
