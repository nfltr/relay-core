// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package grpcfactory

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
	"golang.org/x/net/http2"
)

// TestServerClosesAConnectionThatOpensNoStream: a client that finishes the
// transport handshake (here the HTTP/2 preface and settings; TLS before it
// in production) and then opens no stream, while answering every ping as
// any HTTP/2 library does, is closed within grpc.max_connection_idle. An
// identity arrives only with a stream's headers, so such a connection
// would otherwise hold its memory for good (iroh-relay issue 4630, the
// stage after grpc.connection_timeout).
func TestServerClosesAConnectionThatOpensNoStream(t *testing.T) {
	defer viper.Reset()
	const idle = time.Second
	viper.Set("grpc.max_connection_idle", idle)
	s := NewGRPCServer(testLogger())
	defer s.Stop()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(ln) }()

	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatal(err)
	}
	fr := http2.NewFramer(c, c)
	if err := fr.WriteSettings(); err != nil {
		t.Fatal(err)
	}
	bound := idle + 5*time.Second
	start := time.Now()
	_ = c.SetReadDeadline(start.Add(bound))
	for {
		f, err := fr.ReadFrame()
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("a connection with no stream still open after %v", bound)
		}
		if err != nil { // closed
			t.Logf("closed after %v: %v", time.Since(start).Round(time.Millisecond), err)
			return
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				_ = fr.WriteSettingsAck()
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				_ = fr.WritePing(true, f.Data)
			}
		}
	}
}
