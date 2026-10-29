// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Package relaycoretest starts a relay core in the test's own process, on
// loopback ports the kernel picks, as net/http/httptest starts an HTTP
// server: one call, addresses and an API key back, stopped when the test
// ends.
//
// The relay is compose.Serve with the frontends nfltr-relay-core links
// (HTTP ingress and SNI passthrough), TCP sessions on, no TLS, no metrics
// port and one generated key. Agents (github.com/nfltr/relay/client/agent)
// connect to GRPC with Key; the key's clients reach those agents on Proxy
// (data connections, /connect/, HTTP ingress).
//
// The relay core reads its settings from the process's configuration
// (platform/config and viper). The first New registers that configuration
// if the process has not (platform/config.SetDefaultFlags), then sets
// tcp.enabled and a one-second drain for the whole process; relays started
// by New in one process share them.
package relaycoretest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/core/frontend"
	httpingress "github.com/nfltr/relay-core/frontend/http"
	"github.com/nfltr/relay-core/frontend/sni"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay-core/platform/config"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Relay is a running relay core.
type Relay struct {
	// GRPC is the agents' address (host:port, plaintext gRPC).
	GRPC string
	// Proxy is the clients' address: data connections and /connect/
	// sessions to the key's agents, and the key's HTTP requests to them.
	Proxy string
	// Internal serves /ready and /health.
	Internal string
	// Key is the API key agents and clients authenticate with: agents
	// connect under names in its namespace, and its clients reach them.
	Key string
}

var configure sync.Once

// New starts a relay core and stops it when tb's test ends (tb.Cleanup):
// the run is cancelled and New's cleanup waits until it drained, at most
// about one second past the last open client session. A relay that cannot
// start fails the test (tb.Fatalf).
func New(tb testing.TB) *Relay {
	tb.Helper()
	configure.Do(func() {
		if flag.CommandLine.Lookup(config.GrpcServerPort) == nil {
			args := os.Args
			os.Args = os.Args[:1] // the configuration parses the process's argv; a test's flags are testing's
			config.SetDefaultFlags()
			os.Args = args
		}
		viper.Set("tcp.enabled", true)
		viper.Set("termination.grace.periodseconds", 1)
	})
	secret := make([]byte, 24)
	if _, err := rand.Read(secret); err != nil {
		tb.Fatalf("relaycoretest: %v", err)
	}
	r := &Relay{Key: "rpc_" + hex.EncodeToString(secret)}
	keys := keyfile.NewAPIKeyStore()
	keys.Add("test", r.Key)

	var ls compose.Ports
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal} {
		var err error
		if *l, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			tb.Fatalf("relaycoretest: %v", err)
		}
	}
	r.GRPC, r.Proxy, r.Internal = ls.GRPC.Addr().String(), ls.Proxy.Addr().String(), ls.Internal.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	frontends := []frontend.Frontend{httpingress.Frontend{}, sni.Frontend{}}
	go func() {
		done <- compose.Serve(ctx, slog.New(slog.DiscardHandler), ls, frontends, keyfile.Provider{Store: keys})
	}()
	ready := make(chan error, 1)
	go func() {
		res, err := (&http.Client{Timeout: 30 * time.Second}).Get("http://" + r.Internal + "/ready")
		if err == nil {
			_ = res.Body.Close()
		}
		ready <- err
	}()
	select {
	case err := <-done:
		cancel()
		tb.Fatalf("relaycoretest: the relay core did not start: %v", err)
	case err := <-ready:
		if err != nil {
			cancel()
			tb.Fatalf("relaycoretest: the relay core is not ready: %v", err)
		}
	}
	tb.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				tb.Errorf("relaycoretest: the relay core ended: %v", err)
			}
		case <-time.After(30 * time.Second):
			tb.Errorf("relaycoretest: the relay core did not stop within 30 s")
		}
	})
	return r
}
