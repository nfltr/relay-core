// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command nfltr-relay-core runs the relay core (compose.Serve) on its own:
// one replica's sessions (core/session) and in-process directory, its gRPC
// agent port authenticated by the key file (keyfile, grpcauth), the proxy
// port where key holders reach their agents (frontend/connect, HTTP
// ingress), and the internal (/ready, /health) and metrics ports. It links
// no database and no hosted service.
//
// Agents connect and register; with tcp.enabled, the key's clients open
// sessions to their agents' ports and services on the proxy port's
// /connect/ (data connections, HTTP/2 streams, WebSocket). On SIGTERM it
// drains: no new client, open sessions run to the grace period's end. The
// proxy and gRPC ports take the operator's certificate (tls.enable,
// tls.cert_file, tls.key_file); without TLS they listen on loopback only,
// and a non-loopback listen.address is refused.
//
//	nfltr-relay-core serve              zero-config: loopback only, TCP sessions on,
//	                                    the key file ./nfltr-relay-core.keys
//	                                    (created 0600 with a generated key when absent)
//	nfltr-relay-core serve --from-env   every setting from the environment
//	                                    (PROXY_PORT, GRPC_PORT, INTERNAL_PORT,
//	                                    METRICS_PORT, LISTEN_ADDRESS,
//	                                    SECURITY_APIKEY_FILE, NFLTR_API_KEY,
//	                                    TCP_ENABLED, TLS_ENABLE, ...)
//	nfltr-relay-core version
//
// Under serve, the environment still overrides each zero-config default.
package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/internal/platform/logging"
	"github.com/nfltr/relay-core/platform/buildinfo"
	"github.com/nfltr/relay-core/platform/config"
	_ "github.com/nfltr/relay-core/platform/nondumpable"
	"github.com/nfltr/relay-core/tlsconfig"
	"github.com/spf13/viper"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	switch {
	case len(args) == 1 && (args[0] == "version" || args[0] == "--version"):
		_, _ = fmt.Fprintf(stdout, "nfltr-relay-core %s\n", buildinfo.Version)
		return 0
	case len(args) == 0 || args[0] != "serve" || len(args) > 2 || (len(args) == 2 && args[1] != "--from-env"):
		_, _ = fmt.Fprint(stderr, "usage: nfltr-relay-core serve [--from-env] | version\n")
		return 2
	}
	os.Args = os.Args[:1] // config parses the process argv
	config.SetDefaultFlags()
	logger := logging.New(viper.GetString("logging.level"))
	var err error
	if len(args) == 1 {
		err = zeroConfig(logger)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var tlsCfg *tls.Config
	if err == nil {
		tlsCfg, err = serverTLS()
	}
	var ls compose.Ports
	if err == nil {
		ls, err = listen(tlsCfg)
	}
	if err == nil {
		err = compose.Serve(ctx, logger, ls, frontends)
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "nfltr-relay-core: %v\n", err)
		return 1
	}
	return 0
}

// zeroKeyFile is the key file of a zero-config start, in the working
// directory.
const zeroKeyFile = "nfltr-relay-core.keys"

// zeroConfig makes the defaults of `serve` without --from-env: loopback
// ports, TCP sessions on, and the key file zeroKeyFile, created 0600 with
// one generated key (plaintext, so a local agent and client can read it)
// when it does not exist. The environment still overrides each default;
// the key file then is the one security.apikey.file names.
func zeroConfig(logger *slog.Logger) error {
	viper.SetDefault("listen.address", "127.0.0.1")
	viper.SetDefault("internal.address", "127.0.0.1")
	viper.SetDefault("tcp.enabled", true)
	viper.SetDefault("security.apikey.file", zeroKeyFile)
	path := viper.GetString("security.apikey.file")
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		return err // it exists (nil), or it cannot be read
	}
	key := make([]byte, 24)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- the operator's key file path
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "# nfltr-relay-core zero-config key: <agent id>:<key>; agents and clients send the key\nquickstart:rpc_%s\n", hex.EncodeToString(key))
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		logger.Info("generated an API key", "file", path)
	}
	return err
}

// serverTLS is the TLS of the proxy and gRPC ports (and the SNI router's
// non-E2EE names): the operator's certificate (tls.enable with
// tls.cert_file and tls.key_file), as nfltr-relay loads it; nil without
// tls.enable, which listen.address must then keep on loopback, as
// nfltr-relay serve does. The internal and metrics ports stay plain, as
// the relay's (probes, scrapes on internal.address).
func serverTLS() (*tls.Config, error) {
	if !viper.GetBool("tls.enable") {
		if err := tlsconfig.RefusePlaintext(viper.GetString("listen.address")); err != nil {
			return nil, fmt.Errorf("%w: set TLS_ENABLE=true with TLS_CERT_FILE and TLS_KEY_FILE (tls.enable, tls.cert_file, tls.key_file), or LISTEN_ADDRESS=127.0.0.1", err)
		}
		return nil, nil
	}
	if viper.GetBool("tls.auto") || viper.GetBool("tls.acme") {
		return nil, errors.New("tls.auto and tls.acme are nfltr-relay's: the core takes the operator's certificate (tls.cert_file, tls.key_file)")
	}
	return tlsconfig.Load(viper.GetString("tls.cert_file"), viper.GetString("tls.key_file"))
}

// listen binds the configured ports as nfltr-relay does (compose.Listen:
// every listen.address entry, "0" kernel-chosen; the SNI router's port with
// tcp sessions and passthrough on a base domain, never a mux on the gRPC
// port), the proxy and gRPC ports under tlsCfg (nil: plaintext).
func listen(tlsCfg *tls.Config) (ls compose.Ports, err error) {
	listeners, _, err := compose.Listen(false)
	if err != nil {
		return ls, err
	}
	port := func(key string) net.Listener { return listeners[viper.GetString(key)] }
	ls.Proxy, ls.GRPC, ls.Internal, ls.Metrics = port(config.ProxyServerPort), port(config.GrpcServerPort), port("internal.port"), port("metrics.port")
	if ls.Proxy == nil || ls.GRPC == nil || ls.Internal == nil {
		for _, l := range listeners {
			_ = l.Close()
		}
		return ls, errors.New("the core serves the proxy, gRPC and internal ports: http.server.enable, rpc.server.enable and internal.enable")
	}
	if l := port(config.SNIRouterPort); l != nil && compose.BoundPorts(false)[config.SNIRouterPort] != "" {
		ls.Ports = map[string]net.Listener{config.SNIRouterPort: l}
	}
	ls.Proxy, ls.GRPC = tlsconfig.WrapListener(ls.Proxy, tlsCfg), tlsconfig.WrapGRPCListener(ls.GRPC, tlsCfg)
	return ls, nil
}
