// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/core/identity"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay-core/platform/config"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// TestProxyClosesAnIdleKeepAliveConnection: a client that made one request
// on the proxy port and then keeps its connection open, sending nothing,
// is closed after http.server.idle_timeout, not held for good (net/http
// has no idle bound when IdleTimeout and ReadTimeout are both zero).
func TestProxyClosesAnIdleKeepAliveConnection(t *testing.T) {
	if flag.CommandLine.Lookup(config.GrpcServerPort) == nil {
		args := os.Args
		os.Args = os.Args[:1]
		config.SetDefaultFlags()
		os.Args = args
	}
	const idle = time.Second
	prev := viper.Get("http.server.idle_timeout")
	viper.Set("http.server.idle_timeout", idle)
	t.Cleanup(func() { viper.Set("http.server.idle_timeout", prev) })
	keys := keyfile.NewAPIKeyStore()
	keys.Add(keyfile.Machine{ID: identity.NewMachineID(), Name: "demo"}, "rpc_example-key")
	var ls compose.Ports
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal} {
		var err error
		if *l, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- compose.Serve(ctx, slog.New(slog.DiscardHandler), ls, nil, keyfile.Provider{Store: keys})
	}()
	t.Cleanup(func() { stop(); <-done })

	c, err := net.Dial("tcp", ls.Proxy.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := io.WriteString(c, "GET /x HTTP/1.1\r\nHost: relay.test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	res, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	if res.Close {
		t.Fatalf("the proxy closed after its %d response; want keep-alive", res.StatusCode)
	}
	start := time.Now()
	bound := idle + 3*time.Second
	_ = c.SetReadDeadline(start.Add(bound))
	if _, err := io.Copy(io.Discard, br); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("an idle keep-alive connection still open after %v", bound)
	}
	t.Logf("closed after %v", time.Since(start).Round(time.Millisecond))
}
