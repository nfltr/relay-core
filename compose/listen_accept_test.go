// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package compose

import (
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/nfltr/relay-core/internal/platform/acceptloop/acceptlooptest"
	"github.com/nfltr/relay-core/platform/connlimit"
)

// countingListener counts the Accept calls its server makes.
type countingListener struct {
	net.Listener
	calls atomic.Int32
	again chan struct{} // closed at the second call
}

func (l *countingListener) Accept() (net.Conn, error) {
	if l.calls.Add(1) == 2 {
		close(l.again)
	}
	return l.Listener.Accept()
}

// TestPublicPortsSurviveEMFILE: the proxy (net/http) and gRPC servers serve
// the public ports through the composition's listener wrappers (the
// connection limit, the merged listen addresses); an EMFILE from below
// reaches them as a temporary error, so they back off and accept again
// instead of returning, which would stop the port (the backoff itself is
// theirs: net/http's and grpc-go's 5 ms doubling to 1 s).
func TestPublicPortsSurviveEMFILE(t *testing.T) {
	wraps := map[string]func(net.Listener) net.Listener{
		"connlimit": func(l net.Listener) net.Listener {
			return connlimit.Wrap(l, connlimit.Config{Listener: "test", MaxPerIP: 1, Rate: 1, Burst: 1})
		},
		"merged": func(l net.Listener) net.Listener { return newMultiListener([]net.Listener{l}) },
	}
	servers := map[string]func(net.Listener) (serve func() error, stop func()){
		"http": func(l net.Listener) (func() error, func()) {
			s := &http.Server{ReadHeaderTimeout: time.Second}
			return func() error { return s.Serve(l) }, func() { _ = s.Close() }
		},
		"grpc": func(l net.Listener) (func() error, func()) {
			s := grpc.NewServer()
			return func() error { return s.Serve(l) }, s.Stop
		},
	}
	for wname, wrap := range wraps {
		for sname, server := range servers {
			t.Run(sname+"/"+wname, func(t *testing.T) {
				l := &countingListener{Listener: wrap(acceptlooptest.New()), again: make(chan struct{})}
				serve, stop := server(l)
				served := make(chan error, 1)
				go func() { served <- serve() }()
				select {
				case err := <-served:
					t.Fatalf("the server returned after one EMFILE: %v", err)
				case <-l.again:
				case <-time.After(10 * time.Second):
					t.Fatal("the server did not accept again after an EMFILE")
				}
				stop()
				<-served
			})
		}
	}
}
