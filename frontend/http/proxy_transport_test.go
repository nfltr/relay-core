// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package httpingress

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nfltr/relay-core/platform/config"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// TestProxyTransportReusesDispatcherConnectionsUnderLoad (G-L1): every
// proxied request crosses the relay's loopback hop to its dispatcher. With
// the default of 20 idle connections, a burst above 20 in flight closed the
// rest after each request; under large-transfer load the closed connections'
// TIME_WAIT ran the host out of ephemeral ports and small requests got 502
// "bad gateway" (dial: can't assign requested address). Rounds of 200
// concurrent requests must reuse the first round's connections.
func TestProxyTransportReusesDispatcherConnectionsUnderLoad(t *testing.T) {
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	defer viper.Reset()
	config.SetDefaultFlags()

	var opened atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(5 * time.Millisecond) // hold the request so the round overlaps
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	client := &http.Client{Transport: newProxyTransport()}
	const inFlight, rounds = 200, 5
	for range rounds {
		var wg sync.WaitGroup
		for range inFlight {
			wg.Go(func() {
				resp, err := client.Get(srv.URL)
				if err != nil {
					t.Error(err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body) // as the reverse proxy does
				_ = resp.Body.Close()
			})
		}
		wg.Wait()
	}
	if n := opened.Load(); n > inFlight*3/2 {
		t.Fatalf("%d connections opened for %d rounds of %d concurrent requests; want the first round's reused (<= %d)", n, rounds, inFlight, inFlight*3/2)
	}
}
