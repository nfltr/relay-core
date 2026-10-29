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

	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay-core/platform/util"
	"github.com/spf13/viper"
)

// portKeys returns the config keys of the ports a relay listens on (proxy,
// gRPC, internal, metrics, the SNI router's), in PortNumbers order. Peers
// reach this replica's agents over the replica link (peer_link.port, its
// own listener). muxOnGRPC: the binary's SNI frontend sits in front of the
// gRPC port's TLS (frontend.Host.WrapGRPCListener), so it binds no port of
// its own.
func portKeys(muxOnGRPC bool) []string {
	var keys []string
	if viper.GetBool("http.server.enable") {
		keys = append(keys, config.ProxyServerPort)
	}
	if viper.GetBool("rpc.server.enable") {
		keys = append(keys, config.GrpcServerPort)
	}
	if viper.GetBool("internal.enable") {
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
// (nfltr-relay's under tls.enable; the core never has one). Listen binds
// that port, so "0" is a kernel-chosen port the ports file reports.
func sniRouter(muxOnGRPC bool) bool {
	return viper.GetBool("rpc.server.enable") && viper.GetBool("tcp.enabled") &&
		viper.GetBool("e2ee.tls_passthrough.enabled") && viper.GetString("e2ee.tls_passthrough.base_domain") != "" &&
		!muxOnGRPC
}

// listenHostForKey is ListenHost by config key, for ports not yet known.
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

// Listen binds every relay port on its listen host (muxOnGRPC: portKeys). A port configured as "0"
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
	listeners, err = util.GetListeners(fixed, ListenHost)
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
	return listeners, forget, nil
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

// MergeListeners serves several listeners as one (Addr is the first's): the
// relay's own service child dials a loopback listener of its own on the
// gRPC port's server, which the idle exit does not count.
func MergeListeners(all ...net.Listener) net.Listener { return newMultiListener(all) }

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

// PortNumbers are the ports a relay listens on, in portKeys order.
func PortNumbers(muxOnGRPC bool) []string {
	var ports []string
	for _, key := range portKeys(muxOnGRPC) {
		ports = append(ports, viper.GetString(key))
	}
	return ports
}

// ListenHost is the bind address for port: internal.address for the internal
// and metrics ports (admin API, health, /metrics), the first listen.address entry (empty:
// all interfaces) for the public proxy and gRPC listeners (Listen binds the
// other entries too). The standalone deploy binds the first two to loopback.
func ListenHost(port string) string {
	switch port {
	case viper.GetString("internal.port"), viper.GetString("metrics.port"):
		return viper.GetString("internal.address")
	}
	return publicListenHosts()[0]
}
