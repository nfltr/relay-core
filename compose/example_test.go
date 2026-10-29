// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay-core/platform/config"
	flag "github.com/spf13/pflag"
)

// ExampleServe runs a relay core on loopback listeners with one API key
// until its context ends; examples/minimal adds an agent and a client.
func ExampleServe() {
	if flag.CommandLine.Lookup(config.GrpcServerPort) == nil { // the relay's settings, once per process
		args := os.Args
		os.Args = os.Args[:1]
		config.SetDefaultFlags()
		os.Args = args
	}
	keys := keyfile.NewAPIKeyStore()
	keys.Add("demo", "rpc_example-key")

	var ls compose.Ports
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal} {
		*l, _ = net.Listen("tcp", "127.0.0.1:0")
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- compose.Serve(ctx, slog.New(slog.DiscardHandler), ls, nil, keyfile.Provider{Store: keys})
	}()

	res, err := http.Get("http://" + ls.Internal.Addr().String() + "/ready")
	if err == nil {
		_ = res.Body.Close()
		fmt.Println("ready:", res.StatusCode)
	}
	stop()
	fmt.Println("stopped:", <-done)
	// Output:
	// ready: 200
	// stopped: <nil>
}
