// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/nfltr/relay-core/core/session"
	"github.com/spf13/viper"
)

// TestCompositionPortsUntilTheDrain: the internal and metrics ports (pprof
// with profiling.enable) serve, and without an agent port both close when
// the drain starts; Run returns and runs After.
func TestCompositionPortsUntilTheDrain(t *testing.T) {
	viper.Set("profiling.enable", true)
	defer viper.Reset()
	il, _ := net.Listen("tcp", "127.0.0.1:0")
	ml, _ := net.Listen("tcp", "127.0.0.1:0")
	ready := http.NewServeMux()
	ready.HandleFunc("/ready", func(http.ResponseWriter, *http.Request) {})
	after := make(chan struct{})
	c := &Composition{Core: &session.Core{}, Internal: il, InternalHandler: ready, Metrics: ml, After: func() { close(after) }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	for _, u := range []string{"http://" + il.Addr().String() + "/ready", "http://" + ml.Addr().String() + "/debug/pprof/", "http://" + ml.Addr().String() + "/metrics"} {
		res, err := http.Get(u)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("%s: %d", u, res.StatusCode)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the drain started")
	}
	<-after
	if _, err := http.Get("http://" + ml.Addr().String() + "/metrics"); err == nil {
		t.Fatal("the metrics port still serves after the run")
	}
}
