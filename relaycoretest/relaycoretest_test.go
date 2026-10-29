// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package relaycoretest_test

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nfltr/relay-core/internal/platform/leakcheck"
	"github.com/nfltr/relay-core/relaycoretest"
	"github.com/nfltr/relay/client/agent"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
)

func TestMain(m *testing.M) { os.Exit(leakcheck.Run(m)) }

// echoThrough connects an agent "echo" to r in front of a TCP echo server
// and sends msg through r to it as a client of r's key; it returns what
// came back.
func echoThrough(tb testing.TB, r *relaycoretest.Relay, msg string) string {
	tb.Helper()
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("%v", err)
	}
	tb.Cleanup(func() { _ = echo.Close() })
	go func() {
		for c, err := echo.Accept(); err == nil; c, err = echo.Accept() {
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	a := agent.New(agent.Config{ServerAddr: r.GRPC, AgentID: "echo", APIKey: r.Key, TCPTunnel: true,
		TCPForwardAddr: echo.Addr().String(), Logger: slog.New(slog.DiscardHandler)})
	if err := a.Connect(tb.Context()); err != nil {
		tb.Fatalf("agent: %v", err)
	}
	tb.Cleanup(a.Close)
	nc, err := net.Dial("tcp", r.Proxy)
	if err != nil {
		tb.Fatalf("%v", err)
	}
	m, _, err := dataplane.Upgrade(tb.Context(), nc, "relay", r.Key, 1<<20)
	if err != nil {
		tb.Fatalf("upgrade: %v", err)
	}
	go func() { _ = m.Serve(nil) }()
	tb.Cleanup(func() { _ = m.Close() })
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(50 * time.Millisecond) { // the agent registers its TCP stream just after Connect
		s, err := m.Open(&pb.TCPData{AgentId: "echo", TargetAddr: "22"})
		if err == nil {
			buf := make([]byte, len(msg))
			if _, err = s.Write([]byte(msg)); err == nil {
				if _, err = io.ReadFull(s, buf); err == nil {
					_ = s.Close()
					return string(buf)
				}
			}
		}
		if time.Now().After(deadline) {
			tb.Fatalf("no session through the relay: %v", err)
		}
	}
}

// TestNewCarriesBytes: two relays in one process each carry their own
// key's bytes to their agent.
func TestNewCarriesBytes(t *testing.T) {
	for i := range 2 {
		r := relaycoretest.New(t)
		if got := echoThrough(t, r, fmt.Sprint("hello ", i)); got != fmt.Sprint("hello ", i) {
			t.Fatalf("echoed %q", got)
		}
	}
}
