// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

// Command minimal runs a relay core, an agent and a client in one process:
// the client's bytes go through the relay to a TCP echo server behind the
// agent and come back.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/nfltr/relay-core/compose"
	"github.com/nfltr/relay-core/keyfile"
	"github.com/nfltr/relay-core/platform/config"
	"github.com/nfltr/relay/client/agent"
	pb "github.com/nfltr/relay/proto/pb"
	"github.com/nfltr/relay/proto/wire/dataplane"
	"github.com/spf13/viper"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	os.Args = os.Args[:1]
	config.SetDefaultFlags() // the relay's settings: defaults, then the environment
	viper.Set("tcp.enabled", true)
	const key = "rpc_minimal-example-key-0123456789"
	keys := keyfile.NewAPIKeyStore()
	keys.Add("demo", key)

	var ls compose.Ports // the relay's ports: clients, agents, /ready
	for _, l := range []*net.Listener{&ls.Proxy, &ls.GRPC, &ls.Internal} {
		*l = must(net.Listen("tcp", "127.0.0.1:0"))
	}
	go func() {
		log.Fatal(compose.Serve(ctx, slog.New(slog.DiscardHandler), ls, nil, keyfile.Provider{Store: keys}))
	}()

	echo := must(net.Listen("tcp", "127.0.0.1:0")) // the service behind the agent
	go func() {
		for c, err := echo.Accept(); err == nil; c, err = echo.Accept() {
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	a := agent.New(agent.Config{ServerAddr: ls.GRPC.Addr().String(), AgentID: "echo", APIKey: key,
		TCPTunnel: true, TCPForwardAddr: echo.Addr().String(), Logger: slog.New(slog.DiscardHandler)})
	if err := a.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer a.Close()

	m, _ := must2(dataplane.Upgrade(ctx, must(net.Dial("tcp", ls.Proxy.Addr().String())), "relay", key, 1<<20))
	go func() { _ = m.Serve(nil) }()
	for ctx.Err() == nil { // the agent registers its TCP stream just after Connect
		s, err := m.Open(&pb.TCPData{AgentId: "echo", TargetAddr: "22"})
		if err == nil {
			msg := []byte("hello through the relay core")
			buf := make([]byte, len(msg))
			if _, err = s.Write(msg); err == nil {
				if _, err = io.ReadFull(s, buf); err == nil {
					fmt.Printf("echoed: %s\n", buf)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal(ctx.Err())
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatal(err)
	}
	return v
}

func must2[T, U any](v T, u U, err error) (T, U) { return must(v, err), u }
